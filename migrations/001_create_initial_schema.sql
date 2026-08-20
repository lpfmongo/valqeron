-- Valqeron initial schema, consolidated. The pre-1.0 migration history
-- (001–007) was squashed into this single file; the engine is the sole
-- migration runner and the MIGRATIONS array index is the schema version.

-- ================ ISSUERS ================

CREATE TABLE IF NOT EXISTS issuer
(
    id           BLOB PRIMARY KEY,
    name         TEXT,
    status       TEXT    NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    created_at   TEXT    NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    cnpj         TEXT UNIQUE,
    lei          TEXT UNIQUE,
    country_code TEXT CHECK (country_code IS NULL OR length(country_code) = 2),
    CHECK (cnpj IS NULL OR country_code = 'BR')
) STRICT, WITHOUT ROWID;

-- ================ SECURITIES ================

CREATE TABLE IF NOT EXISTS security
(
    id                     BLOB PRIMARY KEY,
    issuer_id              BLOB    NOT NULL REFERENCES issuer (id),
    name                   TEXT,
    kind                   TEXT    NOT NULL CHECK (kind IN
                                                   ('COMMON_SHARE', 'PREFERRED_SHARE', 'UNIT', 'DEPOSITARY_RECEIPT')),
    status                 TEXT    NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    created_at             TEXT    NOT NULL,
    version                INTEGER NOT NULL DEFAULT 1,
    isin                   TEXT UNIQUE,
    cfi                    TEXT CHECK (cfi IS NULL OR length(cfi) = 6),
    underlying_security_id BLOB REFERENCES security (id),
    dr_ratio_receipts      INTEGER CHECK (dr_ratio_receipts IS NULL OR dr_ratio_receipts > 0),
    dr_ratio_underlying    INTEGER CHECK (dr_ratio_underlying IS NULL OR dr_ratio_underlying > 0),
    CHECK ((dr_ratio_receipts IS NULL) = (dr_ratio_underlying IS NULL)),
    CHECK (kind = 'DEPOSITARY_RECEIPT' OR
           (underlying_security_id IS NULL AND dr_ratio_receipts IS NULL AND dr_ratio_underlying IS NULL)),
    CHECK (underlying_security_id IS NULL OR underlying_security_id <> id)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_security_issuer_id ON security (issuer_id);

CREATE INDEX IF NOT EXISTS idx_security_underlying_security_id ON security (underlying_security_id)
    WHERE underlying_security_id IS NOT NULL;

-- ================ SYNC CURSORS ================
-- Per-source sync progress: scheduling position (through_slot), data
-- coverage (through_target), and failure/cooldown state. Never pruned.

CREATE TABLE IF NOT EXISTS sync_cursor
(
    source               TEXT    NOT NULL PRIMARY KEY CHECK (length(source) > 0),
    through_slot         TEXT    NOT NULL,
    through_target       TEXT    NOT NULL,
    cooldown_until       TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    last_outcome         TEXT CHECK (last_outcome IS NULL OR last_outcome IN ('SYNCED', 'NOT_READY', 'FAILED')),
    last_error           TEXT,
    updated_at           TEXT    NOT NULL
) STRICT, WITHOUT ROWID;

-- ================ BACKGROUND TASKS ================
-- Four tables, one owner each:
--
--   task_registry   catalog + operator intent + settings
--   task_queue      live work only (a PENDING row is the durable alarm)
--   task_execution  terminal run history (pruned after retention)
--   task_stat       prune-proof per-kind aggregates
--
-- On task_registry, `enabled` is operator intent, preserved across boots.
-- The settings columns hold the operator's overrides of a task's
-- scheduling knobs. NULL means "code-owned": the boot reconcile fills a
-- NULL with the task's code default when the task declares that knob
-- tunable, and leaves it NULL forever when it does not (e.g. a period
-- derived from the environment at each boot). Non-NULL values are never
-- rewritten by the reconcile — operator intent survives restarts and
-- upgrades.

CREATE TABLE IF NOT EXISTS task_registry
(
    kind                TEXT    NOT NULL PRIMARY KEY CHECK (length(kind) > 0),
    category            TEXT    NOT NULL CHECK (category IN ('ENGINE_SYSTEM', 'FINANCE_DATA_SYNC', 'OTHER')),
    trigger_kind        TEXT    NOT NULL CHECK (trigger_kind IN ('INTERVAL', 'RECURRING', 'SYNC')),
    tracking            TEXT    NOT NULL CHECK (tracking IN ('DURABLE', 'EPHEMERAL')),
    schedule            TEXT    NOT NULL CHECK (length(schedule) > 0),
    source              TEXT,
    log_policy          TEXT    NOT NULL DEFAULT 'ALL' CHECK (log_policy IN ('ALL', 'FAILURES_ONLY')),
    enabled             INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    period_secs         INTEGER CHECK (period_secs IS NULL OR period_secs > 0),
    at_local            TEXT    CHECK (at_local IS NULL OR length(at_local) = 5),
    recurrence          TEXT    CHECK (recurrence IS NULL OR length(recurrence) > 0),
    cooldown_secs       INTEGER CHECK (cooldown_secs IS NULL OR cooldown_secs >= 0),
    max_backfill_days   INTEGER CHECK (max_backfill_days IS NULL OR max_backfill_days >= 0),
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
