package dbmigrate

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestMigrationUpIsRepeatable(t *testing.T) {
	adminDSN := os.Getenv("DATABASE_URL")
	if adminDSN == "" {
		t.Skip("DATABASE_URL 未设置，迁移测试未运行")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("连接管理库失败: %v", err)
	}
	defer admin.Close(ctx)

	const dbName = "coderisk_migrate_slice"
	if _, err := admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, dbName); err != nil {
		t.Fatalf("断开旧连接失败: %v", err)
	}
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS `+dbName); err != nil {
		t.Fatalf("删除测试库失败: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+dbName); err != nil {
		t.Fatalf("创建测试库失败: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer ccancel()
		_, _ = admin.Exec(cctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, dbName)
		_, _ = admin.Exec(cctx, `DROP DATABASE IF EXISTS `+dbName)
	})

	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Database = dbName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("连接测试库失败: %v", err)
	}
	defer conn.Close(ctx)

	dir := migrationsDir(t)
	if err := ApplyUp(ctx, conn, dir); err != nil {
		t.Fatalf("首次迁移失败: %v", err)
	}
	if err := ApplyUp(ctx, conn, dir); err != nil {
		t.Fatalf("重复迁移失败: %v", err)
	}
	assertVersionCount(t, ctx, conn, 1)
	assertTable(t, ctx, conn, "tasks", true)

	if _, err := conn.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, status, created_at, updated_at)
		VALUES ('00000000-0000-4000-8000-000000000001', 'Owner@Example.com', 'hash', 'ACTIVE', now(), now())
	`); err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, status, created_at, updated_at)
		VALUES ('00000000-0000-4000-8000-000000000002', 'owner@example.com', 'hash', 'ACTIVE', now(), now())
	`)
	if err == nil {
		t.Fatal("大小写不同的重复邮箱应当被拒绝")
	}

	_, err = conn.Exec(ctx, `
		INSERT INTO tasks (id, user_id, thread_id, status, current_attempt, created_at, updated_at)
		VALUES ('00000000-0000-4000-8000-000000000010', '00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000099', 'RUNNING', 1, now(), now())
	`)
	if err == nil {
		t.Fatal("新建任务直接进入 RUNNING 应当被拒绝")
	}

	if err := ApplyDown(ctx, conn, dir); err != nil {
		t.Fatalf("回滚失败: %v", err)
	}
	assertVersionCount(t, ctx, conn, 0)
	assertTable(t, ctx, conn, "tasks", false)

	if err := ApplyUp(ctx, conn, dir); err != nil {
		t.Fatalf("回滚后再次迁移失败: %v", err)
	}
	assertVersionCount(t, ctx, conn, 1)
	assertTable(t, ctx, conn, "tasks", true)
	assertTable(t, ctx, conn, "task_attempts", true)
	assertTable(t, ctx, conn, "outbox_events", true)
}

func migrationsDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位迁移目录")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations")
}

func assertVersionCount(t *testing.T, ctx context.Context, conn *pgx.Conn, want int) {
	t.Helper()
	var got int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("schema_migrations 行数 = %d，期望 %d", got, want)
	}
}

func assertTable(t *testing.T, ctx context.Context, conn *pgx.Conn, name string, want bool) {
	t.Helper()
	var got bool
	err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+name).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("表 %s 存在=%v，期望 %v", name, got, want)
	}
}
