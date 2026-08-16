CREATE TABLE IF NOT EXISTS task_registration
(
    kind                TEXT    NOT NULL PRIMARY KEY CHECK (length(kind) > 0),
    category            TEXT    NOT NULL CHECK (category IN ('ENGINE_SYSTEM', 'FINANCE_DATA_SYNC', 'OTHER')),
    tier                TEXT    NOT NULL CHECK (tier IN ('INTERVAL', 'RECURRING', 'SYNC')),
    tracking            TEXT    NOT NULL CHECK (tracking IN ('DURABLE', 'EPHEMERAL')),
    schedule            TEXT    NOT NULL CHECK (length(schedule) > 0),
    source              TEXT,
    log_policy          TEXT    NOT NULL DEFAULT 'ALL' CHECK (log_policy IN ('ALL', 'FAILURES_ONLY')),
    config_enabled      INTEGER NOT NULL DEFAULT 1 CHECK (config_enabled IN (0, 1)),
    paused              INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    registered          INTEGER NOT NULL DEFAULT 1 CHECK (registered IN (0, 1)),
    last_run_at         TEXT,
    last_outcome        TEXT CHECK (last_outcome IS NULL OR last_outcome IN ('SUCCEEDED', 'FAILED')),
    last_error          TEXT,
    total_runs          INTEGER NOT NULL DEFAULT 0 CHECK (total_runs >= 0),
    total_failures      INTEGER NOT NULL DEFAULT 0 CHECK (total_failures >= 0),
    first_registered_at TEXT    NOT NULL,
    updated_at          TEXT    NOT NULL
) STRICT, WITHOUT ROWID;
