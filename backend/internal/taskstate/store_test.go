package taskstate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"coderisk/backend/internal/dbmigrate"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMain(m *testing.M) {
	if os.Getenv("DATABASE_URL") == "" {
		os.Exit(m.Run())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	if err := dbmigrate.ApplyUp(ctx, conn.Conn(), migrationsDir()); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestCreateAllocatesOnlyFirstAttempt(t *testing.T) {
	svc, ctx := newService(t)
	userID, threadID := seedUserThread(t, ctx, svc)
	eventID := mustID(t)
	task, err := svc.CreateInitialAttempt(ctx, userID, threadID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if task.Status != StatusPending || task.CurrentAttempt != 1 {
		t.Fatalf("创建结果 = %s/%d", task.Status, task.CurrentAttempt)
	}
	assertAttemptCount(t, ctx, svc, task.ID, 1)
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisRequested, 1)
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisCancel, 0)

	again, err := svc.MarkQueued(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkQueued(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if again.Status != StatusQueued || again.CurrentAttempt != 1 {
		t.Fatalf("重复发布后 = %s/%d", again.Status, again.CurrentAttempt)
	}
	assertAttemptCount(t, ctx, svc, task.ID, 1)
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisRequested, 1)
}

func TestCancelVersusCompleteAllowsOneTerminal(t *testing.T) {
	svc, ctx := newService(t)
	for i := 0; i < 25; i++ {
		task := runningTask(t, ctx, svc)
		var wg sync.WaitGroup
		errCh := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := svc.Cancel(ctx, task.ID)
			errCh <- err
		}()
		go func() {
			defer wg.Done()
			_, err := svc.Complete(ctx, task.ID, 1, task.EventID, StatusSucceeded)
			errCh <- err
		}()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil && !errors.Is(err, ErrStaleCallback) {
				t.Fatal(err)
			}
		}
		got, err := svc.GetTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != StatusCancelled && got.Status != StatusSucceeded {
			t.Fatalf("并发终态 = %s", got.Status)
		}
		if got.CurrentAttempt != 1 {
			t.Fatalf("并发后 attempt = %d", got.CurrentAttempt)
		}
		assertAttemptCount(t, ctx, svc, task.ID, 1)
		cancelEvents, err := svc.CountOutbox(ctx, task.ID, EventAnalysisCancel)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == StatusCancelled && cancelEvents != 1 {
			t.Fatalf("取消获胜时应有 1 条取消事件，得到 %d", cancelEvents)
		}
		if got.Status == StatusSucceeded && cancelEvents != 0 {
			t.Fatalf("完成获胜时不应写取消事件，得到 %d", cancelEvents)
		}
		attempt, err := svc.CurrentAttempt(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if Status(attempt.Status) != got.Status {
			t.Fatalf("尝试状态 %s 与任务状态 %s 不一致", attempt.Status, got.Status)
		}
	}
}

func TestCancelThenLateCallbacksDoNotOverwrite(t *testing.T) {
	svc, ctx := newService(t)
	task := runningTask(t, ctx, svc)
	cancelled, err := svc.Cancel(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != StatusCancelled {
		t.Fatalf("取消后状态 = %s", cancelled.Status)
	}
	replay, err := svc.Cancel(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != StatusCancelled {
		t.Fatal(replay.Status)
	}
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisCancel, 1)
	assertAttemptCount(t, ctx, svc, task.ID, 1)

	if _, err := svc.Complete(ctx, task.ID, 1, task.EventID, StatusSucceeded); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("迟到成功回调错误 = %v", err)
	}
	if _, err := svc.Complete(ctx, task.ID, 1, task.EventID, StatusPartialSuccess); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("迟到部分成功回调错误 = %v", err)
	}
	if _, err := svc.Fail(ctx, task.ID, 1, task.EventID, FailureSourceWorker, FailureAnalysisFailed, "late"); !errors.Is(err, ErrStaleCallback) {
		t.Fatalf("迟到失败回调错误 = %v", err)
	}
	got, err := svc.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusCancelled || got.FailureCode != nil || got.CurrentAttempt != 1 {
		t.Fatalf("迟到回调后任务 = %+v", got)
	}
	attempt, err := svc.CurrentAttempt(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != StatusCancelled {
		t.Fatalf("尝试被覆盖为 %s", attempt.Status)
	}
}

func TestCompleteFirstRejectsCancel(t *testing.T) {
	svc, ctx := newService(t)
	task := runningTask(t, ctx, svc)
	done, err := svc.Complete(ctx, task.ID, 1, task.EventID, StatusPartialSuccess)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != StatusPartialSuccess {
		t.Fatal(done.Status)
	}
	current, err := svc.Cancel(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != StatusPartialSuccess {
		t.Fatalf("完成后取消改写了状态: %s", current.Status)
	}
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisCancel, 0)
}

func TestManualRetryStopsAtFourAndFailureDoesNotAllocate(t *testing.T) {
	svc, ctx := newService(t)
	task := runningTask(t, ctx, svc)
	origin, err := svc.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if origin.StartedAt == nil {
		t.Fatal("进入 RUNNING 后应记录 started_at")
	}
	started := origin.StartedAt.UTC().UnixMicro()
	for attempt := 1; attempt <= MaxBusinessAttempts; attempt++ {
		current, getErr := svc.GetTask(ctx, task.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		_, err = svc.Fail(ctx, task.ID, current.CurrentAttempt, currentEvent(t, ctx, svc, task.ID), FailureSourceWorker, FailureModelTimeout, "model")
		if err != nil {
			t.Fatal(err)
		}
		failed, getErr := svc.GetTask(ctx, task.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if failed.CurrentAttempt != attempt || failed.Status != StatusFailed {
			t.Fatalf("失败后 = %s/%d，期望 FAILED/%d", failed.Status, failed.CurrentAttempt, attempt)
		}
		assertAttemptCount(t, ctx, svc, task.ID, attempt)
		if attempt == MaxBusinessAttempts {
			break
		}
		_, err = svc.ManualRetry(ctx, task.ID, mustID(t))
		if err != nil {
			t.Fatal(err)
		}
		queued, getErr := svc.GetTask(ctx, task.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if queued.Status != StatusQueued || queued.CurrentAttempt != attempt+1 || queued.FailureCode != nil || queued.FinishedAt != nil {
			t.Fatalf("重试后 = %+v", queued)
		}
		if queued.StartedAt == nil || queued.StartedAt.UTC().UnixMicro() != started {
			t.Fatal("重试不应重置首次 started_at")
		}
		running, runErr := svc.MarkRunning(ctx, task.ID)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if running.Status != StatusRunning || running.CurrentAttempt != attempt+1 {
			t.Fatal(running.Status, running.CurrentAttempt)
		}
	}
	if _, err := svc.ManualRetry(ctx, task.ID, mustID(t)); !errors.Is(err, ErrAttemptLimitReached) {
		t.Fatalf("第 5 次业务执行错误 = %v", err)
	}
	assertAttemptCount(t, ctx, svc, task.ID, MaxBusinessAttempts)
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisRequested, MaxBusinessAttempts)
}

func TestNonRetryableFailureAndWorkerTimeoutCode(t *testing.T) {
	svc, ctx := newService(t)
	task := runningTask(t, ctx, svc)
	failed, err := svc.Fail(ctx, task.ID, 1, task.EventID, FailureSourceWorker, FailureExecutionTimeout, "timeout")
	if err != nil {
		t.Fatal(err)
	}
	if failed.FailureCode == nil || *failed.FailureCode != string(FailureAnalysisFailed) {
		t.Fatalf("Worker 超时码被保存为 %v", failed.FailureCode)
	}
	if failed.CurrentAttempt != 1 {
		t.Fatal(failed.CurrentAttempt)
	}
	if _, err := svc.ManualRetry(ctx, task.ID, mustID(t)); err != nil {
		t.Fatal(err)
	}

	inputTask := runningTask(t, ctx, svc)
	inputFailed, err := svc.Fail(ctx, inputTask.ID, 1, inputTask.EventID, FailureSourceWorker, FailureInvalidInput, "bad diff")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ManualRetry(ctx, inputTask.ID, mustID(t)); !errors.Is(err, ErrRetryNotAllowed) {
		t.Fatalf("不可重试错误 = %v", err)
	}
	if inputFailed.CurrentAttempt != 1 {
		t.Fatal(inputFailed.CurrentAttempt)
	}

	recoveryTask := runningTask(t, ctx, svc)
	recovery, err := svc.Fail(ctx, recoveryTask.ID, 1, recoveryTask.EventID, FailureSourceRecovery, FailureExecutionInterrupted, "interrupted")
	if err != nil {
		t.Fatal(err)
	}
	if recovery.FailureCode == nil || *recovery.FailureCode != string(FailureExecutionInterrupted) || recovery.CurrentAttempt != 1 {
		t.Fatalf("恢复收口 = %+v", recovery)
	}
}

func TestConcurrentManualRetryAllocatesOnce(t *testing.T) {
	svc, ctx := newService(t)
	task := runningTask(t, ctx, svc)
	if _, err := svc.Fail(ctx, task.ID, 1, task.EventID, FailureSourceRecovery, FailureExecutionTimeout, "timeout"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wins := make(chan struct{}, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.ManualRetry(ctx, task.ID, mustID(t))
			if err == nil {
				wins <- struct{}{}
				return
			}
			if !errors.Is(err, ErrStateConflict) {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(wins)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(wins) != 1 {
		t.Fatalf("并发手动重试成功次数 = %d", len(wins))
	}
	got, err := svc.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusQueued || got.CurrentAttempt != 2 {
		t.Fatalf("并发重试后 = %s/%d", got.Status, got.CurrentAttempt)
	}
	assertAttemptCount(t, ctx, svc, task.ID, 2)
	assertOutbox(t, ctx, svc, task.ID, EventAnalysisRequested, 2)
}

func TestDirectSQLCannotOverwriteTerminalOrAllocateAttempt(t *testing.T) {
	svc, ctx := newService(t)
	task := runningTask(t, ctx, svc)
	if _, err := svc.Cancel(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	pool := svc.pool
	_, err := pool.Exec(ctx, `UPDATE tasks SET status = 'SUCCEEDED', finished_at = now() WHERE id = $1`, task.ID)
	if !isCheckViolation(err) {
		t.Fatalf("直接覆盖 CANCELLED 应被数据库拒绝，得到 %v", err)
	}
	_, err = pool.Exec(ctx, `UPDATE tasks SET current_attempt = current_attempt + 1 WHERE id = $1`, task.ID)
	if !isCheckViolation(err) {
		t.Fatalf("直接增加 attempt 应被数据库拒绝，得到 %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO task_attempts (id, task_id, attempt_no, status, event_id, created_at)
		VALUES ($1, $2, 2, 'PENDING', $3, now())
	`, mustID(t), task.ID, mustID(t))
	if !isCheckViolation(err) {
		t.Fatalf("绕过事务插入下一次尝试应被拒绝，得到 %v", err)
	}
	got, err := svc.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusCancelled || got.CurrentAttempt != 1 {
		t.Fatalf("非法 SQL 后任务 = %s/%d", got.Status, got.CurrentAttempt)
	}
}

func TestCreateRollbackLeavesNoOutbox(t *testing.T) {
	svc, ctx := newService(t)
	tx, err := svc.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taskID := mustID(t)
	now := time.Now().UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO tasks (id, user_id, thread_id, status, current_attempt, created_at, updated_at)
		VALUES ($1, $2, $3, 'PENDING', 1, $4, $4)
	`, taskID, mustID(t), mustID(t), now)
	if err == nil {
		t.Fatal("缺少用户和线程的任务插入应当失败")
	}
	_ = tx.Rollback(ctx)
	var n int
	if err := svc.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, taskID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("失败事务留下了 %d 条 outbox", n)
	}
	var tasks int
	if err := svc.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE id = $1`, taskID).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if tasks != 0 {
		t.Fatal("失败事务留下了任务")
	}
}

type startedTask struct {
	ID      string
	EventID string
}

func runningTask(t *testing.T, ctx context.Context, svc *Service) startedTask {
	t.Helper()
	userID, threadID := seedUserThread(t, ctx, svc)
	eventID := mustID(t)
	task, err := svc.CreateInitialAttempt(ctx, userID, threadID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkQueued(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkRunning(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	return startedTask{ID: task.ID, EventID: eventID}
}

func seedUserThread(t *testing.T, ctx context.Context, svc *Service) (string, string) {
	t.Helper()
	userID := mustID(t)
	threadID := mustID(t)
	now := time.Now().UTC()
	if _, err := svc.pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, status, created_at, updated_at)
		VALUES ($1, $2, 'test-hash', 'ACTIVE', $3, $3)
	`, userID, userID+"@example.com", now); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.pool.Exec(ctx, `
		INSERT INTO threads (id, user_id, created_at) VALUES ($1, $2, $3)
	`, threadID, userID, now); err != nil {
		t.Fatal(err)
	}
	return userID, threadID
}

func newService(t *testing.T) (*Service, context.Context) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL 未设置，数据库测试未运行")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return NewService(pool), ctx
}

func assertAttemptCount(t *testing.T, ctx context.Context, svc *Service, taskID string, want int) {
	t.Helper()
	got, err := svc.CountAttempts(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("attempt 行数 = %d，期望 %d", got, want)
	}
}

func assertOutbox(t *testing.T, ctx context.Context, svc *Service, taskID, eventType string, want int) {
	t.Helper()
	got, err := svc.CountOutbox(ctx, taskID, eventType)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s 事件数 = %d，期望 %d", eventType, got, want)
	}
}

func currentEvent(t *testing.T, ctx context.Context, svc *Service, taskID string) string {
	t.Helper()
	attempt, err := svc.CurrentAttempt(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	return attempt.EventID
}

func mustID(t *testing.T) string {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func isCheckViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23514"
}

func migrationsDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("无法定位迁移目录")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "migrations")
}
