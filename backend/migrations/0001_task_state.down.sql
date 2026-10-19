DROP TRIGGER IF EXISTS task_attempts_before_update ON task_attempts;
DROP TRIGGER IF EXISTS task_attempts_before_insert ON task_attempts;
DROP TRIGGER IF EXISTS tasks_before_update ON tasks;
DROP TRIGGER IF EXISTS tasks_before_insert ON tasks;

DROP FUNCTION IF EXISTS task_attempts_reject_illegal_change();
DROP FUNCTION IF EXISTS task_attempts_reject_illegal_insert();
DROP FUNCTION IF EXISTS tasks_reject_illegal_change();
DROP FUNCTION IF EXISTS tasks_reject_illegal_insert();

DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS task_attempts;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS threads;
DROP TABLE IF EXISTS users;
