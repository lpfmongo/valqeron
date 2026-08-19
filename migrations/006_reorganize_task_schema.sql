-- Reorganized background-task data model. One fused queue+history table
-- (background_task) and a catalog carrying denormalised run summaries
-- (task_registration) become four tables with one owner each:
--
--   task_registry   catalog + operator intent  (was task_registration)
--   task_queue      live work only             (was background_task, active rows)
--   task_execution  terminal run history       (was background_task, terminal rows)
--   task_stat       prune-proof run aggregates (was task_registration summary)
--
-- Data is carried over; the old tables are dropped at the end.

CREATE TABLE IF NOT EXISTS task_registry
(
    kind                TEXT    NOT NULL PRIMARY KEY CHECK (length(kind) > 0),
    category            TEXT    NOT NULL CHECK (category IN ('ENGINE_SYSTEM', 'FINANCE_DATA_SYNC', 'OTHER')),
    trigger_kind        TEXT    NOT NULL CHECK (trigger_kind IN ('INTERVAL', 'RECURRING', 'SYNC')),
    tracking            TEXT    NOT NULL CHECK (tracking IN ('DURABLE', 'EPHEMERAL')),
    schedule            TEXT    NOT NULL CHECK (length(schedule) > 0),
    source              TEXT,
    log_policy          TEXT    NOT NULL DEFAULT 'ALL' CHECK (log_policy IN ('ALL', 'FAILURES_ONLY')),
    config_enabled      INTEGER NOT NULL DEFAULT 1 CHECK (config_enabled IN (0, 1)),
    paused              INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    registered          INTEGER NOT NULL DEFAULT 1 CHECK (registered IN (0, 1)),
    first_registered_at TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS task_queue
(
    id               BLOB PRIMARY KEY,
    kind             TEXT    NOT NULL CHECK (length(kind) > 0),
    status           TEXT    NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING')),
    payload          TEXT,
    scheduled_at     TEXT    NOT NULL,
    started_at       TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    retry_delay_secs INTEGER NOT NULL DEFAULT 0 CHECK (retry_delay_secs >= 0),
    last_error       TEXT,
    created_at       TEXT    NOT NULL,
    updated_at       TEXT    NOT NULL,
    version          INTEGER NOT NULL DEFAULT 1
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_task_queue_due ON task_queue (status, scheduled_at);

CREATE INDEX IF NOT EXISTS idx_task_queue_kind ON task_queue (kind, scheduled_at);

CREATE TABLE IF NOT EXISTS task_execution
(
    id           BLOB PRIMARY KEY,
    kind         TEXT    NOT NULL CHECK (length(kind) > 0),
    outcome      TEXT    NOT NULL CHECK (outcome IN ('SUCCEEDED', 'NOT_READY', 'FAILED')),
    payload      TEXT,
    scheduled_at TEXT    NOT NULL,
    started_at   TEXT,
    finished_at  TEXT    NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    duration_ms  INTEGER CHECK (duration_ms IS NULL OR duration_ms >= 0),
    error        TEXT,
    created_at   TEXT    NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_task_execution_kind ON task_execution (kind, finished_at);

CREATE INDEX IF NOT EXISTS idx_task_execution_finished ON task_execution (finished_at);

CREATE TABLE IF NOT EXISTS task_stat
(
    kind              TEXT    NOT NULL PRIMARY KEY CHECK (length(kind) > 0),
    total_runs        INTEGER NOT NULL DEFAULT 0 CHECK (total_runs >= 0),
    total_failures    INTEGER NOT NULL DEFAULT 0 CHECK (total_failures >= 0),
    total_duration_ms INTEGER NOT NULL DEFAULT 0 CHECK (total_duration_ms >= 0),
    last_run_at       TEXT,
    last_outcome      TEXT CHECK (last_outcome IS NULL OR last_outcome IN ('SUCCEEDED', 'NOT_READY', 'FAILED')),
    last_error        TEXT,
    last_success_at   TEXT,
    last_duration_ms  INTEGER CHECK (last_duration_ms IS NULL OR last_duration_ms >= 0),
    updated_at        TEXT    NOT NULL
) STRICT, WITHOUT ROWID;

INSERT INTO task_registry (kind, category, trigger_kind, tracking, schedule, source, log_policy,
                           config_enabled, paused, registered, first_registered_at, updated_at)
SELECT kind,
       category,
       tier,
       tracking,
       schedule,
       source,
       log_policy,
       config_enabled,
       paused,
       registered,
       first_registered_at,
       updated_at
FROM task_registration;

INSERT INTO task_stat (kind, total_runs, total_failures, total_duration_ms, last_run_at,
                       last_outcome, last_error, last_success_at, last_duration_ms, updated_at)
SELECT kind,
       total_runs,
       total_failures,
       0,
       last_run_at,
       last_outcome,
       last_error,
       CASE WHEN last_outcome = 'SUCCEEDED' THEN last_run_at END,
       NULL,
       updated_at
FROM task_registration
WHERE total_runs > 0
   OR last_run_at IS NOT NULL;

INSERT INTO task_queue (id, kind, status, payload, scheduled_at, started_at, attempts,
                        max_attempts, retry_delay_secs, last_error, created_at, updated_at,
                        version)
SELECT id,
       kind,
       status,
       payload,
       scheduled_at,
       started_at,
       attempts,
       max_attempts,
       retry_delay_secs,
       last_error,
       created_at,
       updated_at,
       version
FROM background_task
WHERE status IN ('PENDING', 'RUNNING');

INSERT INTO task_execution (id, kind, outcome, payload, scheduled_at, started_at, finished_at,
                            attempts, duration_ms, error, created_at)
SELECT id,
       kind,
       status,
       payload,
       scheduled_at,
       started_at,
       COALESCE(finished_at, updated_at),
       attempts,
       CASE
           WHEN started_at IS NOT NULL AND finished_at IS NOT NULL
               THEN CAST(MAX(0, ROUND((julianday(finished_at) - julianday(started_at)) * 86400000.0)) AS INTEGER)
       END,
       last_error,
       created_at
FROM background_task
WHERE status IN ('SUCCEEDED', 'FAILED');

DROP TABLE background_task;

DROP TABLE task_registration;
