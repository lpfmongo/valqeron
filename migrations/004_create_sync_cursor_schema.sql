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
