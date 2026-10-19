package taskstate

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Task struct {
	ID              string
	UserID          string
	ThreadID        string
	Status          Status
	CurrentAttempt  int
	FailureCode     *string
	FailureSummary  *string
	StartedAt       *time.Time
	FinishedAt      *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Attempt struct {
	ID          string
	TaskID      string
	AttemptNo   int
	Status      Status
	FailureCode *string
	EventID     string
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func (s *Service) CreateInitialAttempt(ctx context.Context, userID, threadID, eventID string) (Task, error) {
	taskID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	attemptID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	outboxID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback(ctx)

	var owner string
	err = tx.QueryRow(ctx, `SELECT user_id::text FROM threads WHERE id = $1`, threadID).Scan(&owner)
	if err != nil {
		return Task{}, err
	}
	if owner != userID {
		return Task{}, ErrThreadMismatch
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO tasks (id, user_id, thread_id, status, current_attempt, created_at, updated_at)
		VALUES ($1, $2, $3, 'PENDING', 1, $4, $4)
	`, taskID, userID, threadID, now)
	if err != nil {
		return Task{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO task_attempts (id, task_id, attempt_no, status, event_id, created_at)
		VALUES ($1, $2, 1, 'PENDING', $3, $4)
	`, attemptID, taskID, eventID, now)
	if err != nil {
		return Task{}, err
	}
	if err := insertOutbox(ctx, tx, outboxID, eventID, EventAnalysisRequested, taskID, 1, now); err != nil {
		return Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Task{}, err
	}
	return s.GetTask(ctx, taskID)
}

func (s *Service) MarkQueued(ctx context.Context, taskID string) (Task, error) {
	return s.withTask(ctx, taskID, func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error {
		decision, err := DecideMarkQueued(task.Status, attempt.Status)
		if err != nil {
			return err
		}
		if decision == DecisionIdempotent {
			return nil
		}
		task.Status = StatusQueued
		task.UpdatedAt = now
		return updateTask(ctx, tx, task)
	})
}

func (s *Service) MarkRunning(ctx context.Context, taskID string) (Task, error) {
	return s.withTask(ctx, taskID, func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error {
		decision, err := DecideMarkRunning(task.Status, attempt.Status)
		if err != nil {
			return err
		}
		if decision == DecisionIdempotent {
			return nil
		}
		task.Status = StatusRunning
		if task.StartedAt == nil {
			started := now
			task.StartedAt = &started
		}
		task.UpdatedAt = now
		attempt.Status = StatusRunning
		if attempt.StartedAt == nil {
			started := now
			attempt.StartedAt = &started
		}
		if err := updateTask(ctx, tx, task); err != nil {
			return err
		}
		return updateAttempt(ctx, tx, attempt)
	})
}

func (s *Service) Complete(ctx context.Context, taskID string, attemptNo int, eventID string, result Status) (Task, error) {
	return s.withTask(ctx, taskID, func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error {
		decision, err := DecideComplete(task.Status, attempt.Status, task.CurrentAttempt, attemptNo, attempt.EventID, eventID, result)
		if err != nil {
			return err
		}
		if decision == DecisionIdempotent {
			return nil
		}
		finished := now
		task.Status = result
		task.FinishedAt = &finished
		task.FailureCode = nil
		task.FailureSummary = nil
		task.UpdatedAt = now
		attempt.Status = result
		attempt.FinishedAt = &finished
		attempt.FailureCode = nil
		if err := updateTask(ctx, tx, task); err != nil {
			return err
		}
		return updateAttempt(ctx, tx, attempt)
	})
}

func (s *Service) Fail(ctx context.Context, taskID string, attemptNo int, eventID string, source FailureSource, code FailureCode, summary string) (Task, error) {
	canonical, err := CanonicalFailureCode(source, code)
	if err != nil {
		return Task{}, err
	}
	return s.withTask(ctx, taskID, func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error {
		stored := ""
		if task.FailureCode != nil {
			stored = *task.FailureCode
		}
		decision, err := DecideFail(task.Status, attempt.Status, task.CurrentAttempt, attemptNo, attempt.EventID, eventID, stored, string(canonical))
		if err != nil {
			return err
		}
		if decision == DecisionIdempotent {
			return nil
		}
		finished := now
		codeText := string(canonical)
		task.Status = StatusFailed
		task.FailureCode = &codeText
		task.FailureSummary = &summary
		task.FinishedAt = &finished
		task.UpdatedAt = now
		attempt.Status = StatusFailed
		attempt.FailureCode = &codeText
		attempt.FinishedAt = &finished
		if err := updateTask(ctx, tx, task); err != nil {
			return err
		}
		return updateAttempt(ctx, tx, attempt)
	})
}

func (s *Service) Cancel(ctx context.Context, taskID string) (Task, error) {
	outboxID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	cancelEventID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	return s.withTask(ctx, taskID, func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error {
		decision, err := DecideCancel(task.Status)
		if err != nil {
			return err
		}
		if decision.Kind != DecisionApply {
			return nil
		}
		finished := now
		task.Status = StatusCancelled
		task.FinishedAt = &finished
		task.UpdatedAt = now
		if IsOpenAttempt(attempt.Status) {
			attempt.Status = StatusCancelled
			attempt.FinishedAt = &finished
		}
		if err := updateTask(ctx, tx, task); err != nil {
			return err
		}
		if attempt.Status == StatusCancelled {
			if err := updateAttempt(ctx, tx, attempt); err != nil {
				return err
			}
		}
		return insertOutbox(ctx, tx, outboxID, cancelEventID, EventAnalysisCancel, task.ID, task.CurrentAttempt, now)
	})
}

func (s *Service) ManualRetry(ctx context.Context, taskID, eventID string) (Task, error) {
	attemptID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	outboxID, err := NewID()
	if err != nil {
		return Task{}, err
	}
	return s.withTask(ctx, taskID, func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error {
		var openAttempts int
		if err := tx.QueryRow(ctx, `
			SELECT count(*) FROM task_attempts
			WHERE task_id = $1 AND status IN ('PENDING', 'RUNNING')
		`, taskID).Scan(&openAttempts); err != nil {
			return err
		}
		code := FailureCode("")
		if task.FailureCode != nil {
			code = FailureCode(*task.FailureCode)
		}
		decision, err := DecideManualRetry(task.Status, code, task.CurrentAttempt, openAttempts)
		if err != nil {
			return err
		}
		if decision != DecisionApply {
			return ErrStateConflict
		}
		next := task.CurrentAttempt + 1
		task.Status = StatusQueued
		task.CurrentAttempt = next
		task.FailureCode = nil
		task.FailureSummary = nil
		task.FinishedAt = nil
		task.UpdatedAt = now
		if err := updateTask(ctx, tx, task); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO task_attempts (id, task_id, attempt_no, status, event_id, created_at)
			VALUES ($1, $2, $3, 'PENDING', $4, $5)
		`, attemptID, task.ID, next, eventID, now)
		if err != nil {
			return err
		}
		return insertOutbox(ctx, tx, outboxID, eventID, EventAnalysisRequested, task.ID, next, now)
	})
}

func (s *Service) GetTask(ctx context.Context, taskID string) (Task, error) {
	var task Task
	err := s.pool.QueryRow(ctx, taskSelect+` WHERE id = $1`, taskID).Scan(taskScan(&task)...)
	return task, err
}

func (s *Service) CurrentAttempt(ctx context.Context, taskID string) (Attempt, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT a.id::text, a.task_id::text, a.attempt_no, a.status, a.failure_code, a.event_id::text, a.started_at, a.finished_at
		FROM task_attempts a
		JOIN tasks t ON t.id = a.task_id AND t.current_attempt = a.attempt_no
		WHERE t.id = $1
	`, taskID)
	return scanAttempt(row)
}

func (s *Service) CountAttempts(ctx context.Context, taskID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE task_id = $1`, taskID).Scan(&n)
	return n, err
}

func (s *Service) CountOutbox(ctx context.Context, taskID, eventType string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = $2
	`, taskID, eventType).Scan(&n)
	return n, err
}

func (s *Service) withTask(ctx context.Context, taskID string, fn func(tx pgx.Tx, task *Task, attempt *Attempt, now time.Time) error) (Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Task{}, err
	}
	defer tx.Rollback(ctx)

	var task Task
	err = tx.QueryRow(ctx, taskSelect+` WHERE id = $1 FOR UPDATE`, taskID).Scan(taskScan(&task)...)
	if err != nil {
		return Task{}, err
	}
	attempt, err := lockCurrentAttempt(ctx, tx, task.ID, task.CurrentAttempt)
	if err != nil {
		return Task{}, err
	}
	now := time.Now().UTC()
	if err := fn(tx, &task, &attempt, now); err != nil {
		return Task{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Task{}, err
	}
	return task, nil
}

const taskSelect = `
	SELECT id::text, user_id::text, thread_id::text, status, current_attempt,
	       failure_code, failure_summary, started_at, finished_at, created_at, updated_at
	FROM tasks`

func taskScan(task *Task) []any {
	return []any{
		&task.ID, &task.UserID, &task.ThreadID, &task.Status, &task.CurrentAttempt,
		&task.FailureCode, &task.FailureSummary, &task.StartedAt, &task.FinishedAt,
		&task.CreatedAt, &task.UpdatedAt,
	}
}

func lockCurrentAttempt(ctx context.Context, tx pgx.Tx, taskID string, attemptNo int) (Attempt, error) {
	row := tx.QueryRow(ctx, `
		SELECT id::text, task_id::text, attempt_no, status, failure_code, event_id::text, started_at, finished_at
		FROM task_attempts
		WHERE task_id = $1 AND attempt_no = $2
		FOR UPDATE
	`, taskID, attemptNo)
	return scanAttempt(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAttempt(row scanner) (Attempt, error) {
	var attempt Attempt
	err := row.Scan(
		&attempt.ID, &attempt.TaskID, &attempt.AttemptNo, &attempt.Status,
		&attempt.FailureCode, &attempt.EventID, &attempt.StartedAt, &attempt.FinishedAt,
	)
	return attempt, err
}

func updateTask(ctx context.Context, tx pgx.Tx, task *Task) error {
	_, err := tx.Exec(ctx, `
		UPDATE tasks SET
			status = $2,
			current_attempt = $3,
			failure_code = $4,
			failure_summary = $5,
			started_at = $6,
			finished_at = $7,
			updated_at = $8
		WHERE id = $1
	`, task.ID, task.Status, task.CurrentAttempt, task.FailureCode, task.FailureSummary, task.StartedAt, task.FinishedAt, task.UpdatedAt)
	return err
}

func updateAttempt(ctx context.Context, tx pgx.Tx, attempt *Attempt) error {
	_, err := tx.Exec(ctx, `
		UPDATE task_attempts SET
			status = $2,
			failure_code = $3,
			started_at = $4,
			finished_at = $5
		WHERE id = $1
	`, attempt.ID, attempt.Status, attempt.FailureCode, attempt.StartedAt, attempt.FinishedAt)
	return err
}

func insertOutbox(ctx context.Context, tx pgx.Tx, id, eventID, eventType, taskID string, attemptNo int, now time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			id, event_id, aggregate_type, aggregate_id, event_type, payload, created_at
		) VALUES (
			$1::uuid, $2::uuid, $3, $4::uuid, $5,
			jsonb_build_object('task_id', $6::text, 'attempt_no', $7::int, 'event_id', $8::text),
			$9
		)
	`, id, eventID, AggregateAnalysisTask, taskID, eventType, taskID, attemptNo, eventID, now)
	return err
}
