-- Engine-internal background task queue and execution history.
--
-- Rows are both the schedule (PENDING, due at scheduled_at) and the record of
-- what happened (SUCCEEDED/FAILED with attempts, timings, and last_error).
-- The engine's single-instance lock means exactly one dispatcher consumes
-- this table; claims are still version-guarded like every other write.
CREATE TABLE IF NOT EXISTS background_task
(
    id               BLOB PRIMARY KEY,
    kind             TEXT    NOT NULL CHECK (length(kind) > 0),
    status           TEXT    NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    payload          TEXT,
    scheduled_at     TEXT    NOT NULL,
    started_at       TEXT,
    finished_at      TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    retry_delay_secs INTEGER NOT NULL DEFAULT 0 CHECK (retry_delay_secs >= 0),
    last_error       TEXT,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL,
    version          INTEGER NOT NULL DEFAULT 1
) STRICT, WITHOUT ROWID;

-- Serves the dispatcher's due-task claim (PENDING ordered by due time) and
-- the pruning scan over terminal statuses.
CREATE INDEX IF NOT EXISTS idx_background_task_due ON background_task (status, scheduled_at);

-- Serves per-kind history queries (most recent executions of one kind).
CREATE INDEX IF NOT EXISTS idx_background_task_kind ON background_task (kind, scheduled_at);
