-- 切片：任务状态与业务执行次数。
--
-- 文档冲突（本迁移按已统一的七态实现，不改写设计文档）：
-- 1. 《数据库与任务状态设计》4.4 写 status「只使用六种已确认状态」；
--    5.1 表格未列 CANCELLED，同节正文却写「七种状态」并包含 CANCELLED；
--    第 12 节又写「六种任务状态唯一」。
--    《总体架构与技术决策》明确七种状态，接口取消契约和交接文档同样要求 CANCELLED。
-- 2. 《数据库与任务状态设计》5.2 正文写 FAILED 等终态「没有向外的迁移」，
--    同节状态图、第 6 节和架构文档允许且仅允许合规手动重试 FAILED -> QUEUED。
-- 3. 4.6 将尝试状态冻结为 PENDING/RUNNING/SUCCEEDED/PARTIAL_SUCCESS/FAILED，没有 CANCELLED。
--    若取消后尝试仍保持 RUNNING/PENDING，会与「未结束尝试」冲突；
--    若把尝试记为 FAILED，又与「取消不等于失败」冲突。
--    偏差：task_attempts.status 增加 CANCELLED，只用于关闭被取消的未结束尝试，
--    不计入失败，也不分配新的 attempt_no。

CREATE TABLE users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT users_email_not_blank CHECK (length(btrim(email)) > 0),
    CONSTRAINT users_password_hash_not_blank CHECK (length(password_hash) > 0)
);

CREATE UNIQUE INDEX users_email_lower_uidx ON users (lower(email));

CREATE TABLE threads (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX threads_user_idx ON threads (user_id);

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    thread_id UUID NOT NULL REFERENCES threads (id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN (
        'PENDING',
        'QUEUED',
        'RUNNING',
        'SUCCEEDED',
        'PARTIAL_SUCCESS',
        'FAILED',
        'CANCELLED'
    )),
    current_attempt INTEGER NOT NULL CHECK (current_attempt BETWEEN 1 AND 4),
    failure_code TEXT,
    failure_summary TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX tasks_user_created_idx ON tasks (user_id, created_at DESC);

CREATE TABLE task_attempts (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks (id) ON DELETE RESTRICT,
    attempt_no INTEGER NOT NULL CHECK (attempt_no BETWEEN 1 AND 4),
    status TEXT NOT NULL CHECK (status IN (
        'PENDING',
        'RUNNING',
        'SUCCEEDED',
        'PARTIAL_SUCCESS',
        'FAILED',
        'CANCELLED'
    )),
    failure_code TEXT,
    failure_detail TEXT,
    event_id UUID NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT task_attempts_task_attempt_uidx UNIQUE (task_id, attempt_no),
    CONSTRAINT task_attempts_event_uidx UNIQUE (event_id)
);

CREATE INDEX task_attempts_task_idx ON task_attempts (task_id, attempt_no);

-- 同一任务最多一个未结束尝试。CANCELLED/FAILED/成功态都不算未结束。
CREATE UNIQUE INDEX task_attempts_one_open_uidx
    ON task_attempts (task_id)
    WHERE status IN ('PENDING', 'RUNNING');

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'analysis.requested',
        'analysis.cancel_requested',
        'conversation.reply_requested'
    )),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    last_error TEXT,
    CONSTRAINT outbox_events_event_uidx UNIQUE (event_id)
);

CREATE INDEX outbox_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE OR REPLACE FUNCTION tasks_reject_illegal_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status <> 'PENDING' OR NEW.current_attempt <> 1 THEN
        RAISE EXCEPTION 'new task must start at PENDING attempt 1'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.started_at IS NOT NULL OR NEW.finished_at IS NOT NULL OR NEW.failure_code IS NOT NULL THEN
        RAISE EXCEPTION 'new task cannot carry execution results'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_before_insert
    BEFORE INSERT ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION tasks_reject_illegal_insert();

CREATE OR REPLACE FUNCTION tasks_reject_illegal_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at THEN
        RAISE EXCEPTION 'started_at is immutable once set'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status = 'RUNNING' AND NEW.started_at IS NULL THEN
        RAISE EXCEPTION 'RUNNING requires started_at'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status IN ('PENDING', 'QUEUED', 'RUNNING') AND NEW.finished_at IS NOT NULL THEN
        RAISE EXCEPTION 'non-terminal task cannot have finished_at'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status IN ('SUCCEEDED', 'PARTIAL_SUCCESS', 'FAILED', 'CANCELLED') AND NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'terminal task requires finished_at'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status AND NOT (
        (OLD.status = 'PENDING' AND NEW.status IN ('QUEUED', 'FAILED', 'CANCELLED')) OR
        (OLD.status = 'QUEUED' AND NEW.status IN ('RUNNING', 'FAILED', 'CANCELLED')) OR
        (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'PARTIAL_SUCCESS', 'FAILED', 'CANCELLED')) OR
        (OLD.status = 'FAILED' AND NEW.status = 'QUEUED')
    ) THEN
        RAISE EXCEPTION 'illegal task status transition: % -> %', OLD.status, NEW.status
            USING ERRCODE = '23514';
    END IF;

    IF OLD.status = 'FAILED' AND NEW.status = 'QUEUED' AND NEW.current_attempt <> OLD.current_attempt + 1 THEN
        RAISE EXCEPTION 'FAILED to QUEUED must allocate the next attempt'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.current_attempt IS DISTINCT FROM OLD.current_attempt AND NOT (
        OLD.status = 'FAILED' AND NEW.status = 'QUEUED' AND NEW.current_attempt = OLD.current_attempt + 1
    ) THEN
        RAISE EXCEPTION 'illegal attempt allocation'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.status IN ('SUCCEEDED', 'PARTIAL_SUCCESS', 'CANCELLED') AND (
        NEW.status IS DISTINCT FROM OLD.status OR
        NEW.current_attempt IS DISTINCT FROM OLD.current_attempt OR
        NEW.failure_code IS DISTINCT FROM OLD.failure_code OR
        NEW.failure_summary IS DISTINCT FROM OLD.failure_summary OR
        NEW.finished_at IS DISTINCT FROM OLD.finished_at
    ) THEN
        RAISE EXCEPTION 'immutable terminal task cannot be overwritten'
            USING ERRCODE = '23514';
    END IF;

    IF OLD.status = 'FAILED' AND NEW.status = 'FAILED' AND (
        NEW.failure_code IS DISTINCT FROM OLD.failure_code OR
        NEW.failure_summary IS DISTINCT FROM OLD.failure_summary OR
        NEW.finished_at IS DISTINCT FROM OLD.finished_at OR
        NEW.current_attempt IS DISTINCT FROM OLD.current_attempt
    ) THEN
        RAISE EXCEPTION 'FAILED task cannot be overwritten in place'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_before_update
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION tasks_reject_illegal_change();

CREATE OR REPLACE FUNCTION task_attempts_reject_illegal_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    task_attempt INTEGER;
    existing_count INTEGER;
BEGIN
    IF NEW.status <> 'PENDING' OR NEW.finished_at IS NOT NULL OR NEW.failure_code IS NOT NULL THEN
        RAISE EXCEPTION 'new attempt must start PENDING without a result'
            USING ERRCODE = '23514';
    END IF;

    SELECT current_attempt INTO task_attempt FROM tasks WHERE id = NEW.task_id;
    IF task_attempt IS NULL OR NEW.attempt_no <> task_attempt THEN
        RAISE EXCEPTION 'attempt_no must equal tasks.current_attempt'
            USING ERRCODE = '23514';
    END IF;

    SELECT count(*) INTO existing_count FROM task_attempts WHERE task_id = NEW.task_id;
    IF existing_count <> NEW.attempt_no - 1 THEN
        RAISE EXCEPTION 'attempt_no must be allocated contiguously'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER task_attempts_before_insert
    BEFORE INSERT ON task_attempts
    FOR EACH ROW
    EXECUTE FUNCTION task_attempts_reject_illegal_insert();

CREATE OR REPLACE FUNCTION task_attempts_reject_illegal_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.task_id IS DISTINCT FROM OLD.task_id
        OR NEW.attempt_no IS DISTINCT FROM OLD.attempt_no
        OR NEW.event_id IS DISTINCT FROM OLD.event_id THEN
        RAISE EXCEPTION 'attempt identity is immutable'
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status IS NOT DISTINCT FROM OLD.status THEN
        IF NEW.status IN ('FAILED', 'CANCELLED', 'SUCCEEDED', 'PARTIAL_SUCCESS') AND (
            NEW.failure_code IS DISTINCT FROM OLD.failure_code OR
            NEW.finished_at IS DISTINCT FROM OLD.finished_at
        ) THEN
            RAISE EXCEPTION 'finished attempt cannot be overwritten'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;

    IF NOT (
        (OLD.status = 'PENDING' AND NEW.status IN ('RUNNING', 'FAILED', 'CANCELLED')) OR
        (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED', 'PARTIAL_SUCCESS', 'FAILED', 'CANCELLED'))
    ) THEN
        RAISE EXCEPTION 'illegal attempt status transition: % -> %', OLD.status, NEW.status
            USING ERRCODE = '23514';
    END IF;

    IF NEW.status IN ('SUCCEEDED', 'PARTIAL_SUCCESS', 'FAILED', 'CANCELLED') AND NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'finished attempt requires finished_at'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER task_attempts_before_update
    BEFORE UPDATE ON task_attempts
    FOR EACH ROW
    EXECUTE FUNCTION task_attempts_reject_illegal_change();
