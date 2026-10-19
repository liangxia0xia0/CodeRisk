package dbmigrate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

const schemaMigrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

type migration struct {
	version string
	upSQL   string
	downSQL string
}

// ApplyUp applies pending migrations in version order. Running it again is a no-op
// once each version is recorded.
func ApplyUp(ctx context.Context, conn *pgx.Conn, dir string) error {
	if err := ensureSchemaTable(ctx, conn); err != nil {
		return err
	}
	migrations, err := load(dir)
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.upSQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", m.version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, m.version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", m.version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// ApplyDown rolls back every applied migration, newest first.
func ApplyDown(ctx context.Context, conn *pgx.Conn, dir string) error {
	if err := ensureSchemaTable(ctx, conn); err != nil {
		return err
	}
	migrations, err := load(dir)
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}
	for i := len(migrations) - 1; i >= 0; i-- {
		m := migrations[i]
		if !applied[m.version] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, m.downSQL); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("rollback %s: %w", m.version, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.version); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("forget %s: %w", m.version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func ensureSchemaTable(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, schemaMigrationsTable)
	return err
}

func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		out[version] = true
	}
	return out, rows.Err()
}

func load(dir string) ([]migration, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	byVersion := map[string]*migration{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var version, kind string
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			version = strings.TrimSuffix(name, ".up.sql")
			kind = "up"
		case strings.HasSuffix(name, ".down.sql"):
			version = strings.TrimSuffix(name, ".down.sql")
			kind = "down"
		default:
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		m := byVersion[version]
		if m == nil {
			m = &migration{version: version}
			byVersion[version] = m
		}
		if kind == "up" {
			m.upSQL = string(body)
		} else {
			m.downSQL = string(body)
		}
	}
	out := make([]migration, 0, len(byVersion))
	for _, m := range byVersion {
		if strings.TrimSpace(m.upSQL) == "" || strings.TrimSpace(m.downSQL) == "" {
			return nil, fmt.Errorf("migration %s needs both up and down sql", m.version)
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}
