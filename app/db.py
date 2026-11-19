"""SQLite connection management and schema initialization."""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Iterator

DB_PATH = os.environ.get("DQ_DB_PATH", str(Path.cwd() / "dq.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id    TEXT PRIMARY KEY,
    dataset    TEXT NOT NULL,
    name       TEXT NOT NULL,
    type       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rule_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id     TEXT NOT NULL REFERENCES rules(rule_id),
    version     INTEGER NOT NULL,
    config      TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('draft', 'published', 'archived')),
    change_note TEXT,
    created_at  TEXT NOT NULL,
    UNIQUE (rule_id, version)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    dataset         TEXT NOT NULL,
    source          TEXT NOT NULL CHECK (source IN ('ingest', 'replay')),
    parent_batch_id TEXT REFERENCES batches(batch_id),
    root_batch_id   TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('open', 'published')),
    record_count    INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_root ON batches(root_batch_id);

CREATE TABLE IF NOT EXISTS records (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset    TEXT NOT NULL,
    batch_id   TEXT NOT NULL REFERENCES batches(batch_id),
    record_key TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_lookup ON records(dataset, record_key);
CREATE INDEX IF NOT EXISTS idx_records_batch ON records(batch_id);

CREATE TABLE IF NOT EXISTS executions (
    execution_id            TEXT PRIMARY KEY,
    batch_id                TEXT NOT NULL REFERENCES batches(batch_id),
    dataset                 TEXT NOT NULL,
    created_at              TEXT NOT NULL,
    total                   INTEGER NOT NULL,
    passed                  INTEGER NOT NULL,
    failed                  INTEGER NOT NULL,
    rule_versions_snapshot  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_rule_results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id),
    rule_id      TEXT NOT NULL,
    rule_version INTEGER NOT NULL,
    rule_name    TEXT NOT NULL,
    rule_type    TEXT NOT NULL,
    evaluated    INTEGER NOT NULL,
    failed       INTEGER NOT NULL
);

-- One row per (dataset, record, rule): the *current* quarantine state.
CREATE TABLE IF NOT EXISTS quarantine_entries (
    entry_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset             TEXT NOT NULL,
    record_key          TEXT NOT NULL,
    rule_id             TEXT NOT NULL,
    first_execution_id  TEXT NOT NULL,
    latest_execution_id TEXT NOT NULL,
    latest_batch_id     TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('open', 'fixed')),
    reason              TEXT NOT NULL,
    payload             TEXT NOT NULL,
    first_seen          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE (dataset, record_key, rule_id)
);
CREATE INDEX IF NOT EXISTS idx_qe_status ON quarantine_entries(status, dataset);
CREATE INDEX IF NOT EXISTS idx_qe_batch ON quarantine_entries(latest_batch_id);

-- Append-only: every evaluation outcome is kept forever, even after a record
-- is fixed or re-quarantined in a later repair round.
CREATE TABLE IF NOT EXISTS quarantine_events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id     INTEGER NOT NULL REFERENCES quarantine_entries(entry_id),
    execution_id TEXT NOT NULL,
    batch_id     TEXT NOT NULL,
    rule_version INTEGER NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN
                    ('quarantined', 'still_failing', 'fixed', 're_quarantined')),
    reason       TEXT NOT NULL,
    payload      TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_qev_entry ON quarantine_events(entry_id);
"""


def connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(db_path: str | None = None) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def get_conn() -> Iterator[sqlite3.Connection]:
    """FastAPI dependency: per-request connection with commit/rollback."""
    conn = connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
