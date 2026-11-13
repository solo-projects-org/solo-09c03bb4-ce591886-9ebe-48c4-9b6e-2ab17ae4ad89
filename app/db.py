"""SQLite schema and connection helpers.

The database is the source of truth for consent state and batch state. Generated
CSV files and their digests live on the data volume next to it; the database
records each digest and size so a missing or altered file is never served.
"""

from __future__ import annotations

import contextlib
import sqlite3
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subjects (
    subject_id      TEXT PRIMARY KEY,
    active          INTEGER NOT NULL,              -- 1 = currently consented
    active_since    TEXT NOT NULL,                 -- "" while revoked
    last_changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consent_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id  TEXT NOT NULL,
    active      INTEGER NOT NULL,
    changed_at  TEXT NOT NULL,
    epoch       INTEGER NOT NULL                   -- bumped on every revocation
);

CREATE TABLE IF NOT EXISTS codes (
    subject_id TEXT PRIMARY KEY,
    code       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id             TEXT PRIMARY KEY,
    created_at     TEXT NOT NULL,
    status         TEXT NOT NULL,  -- running | completed | failed | invalidated
    failure_reason TEXT,
    finished_at    TEXT,
    boot_id        TEXT NOT NULL,
    row_count      INTEGER,
    file_path      TEXT,
    file_sha256    TEXT,
    file_size      INTEGER
);

CREATE TABLE IF NOT EXISTS batch_subjects (
    batch_id      TEXT NOT NULL,
    subject_id    TEXT NOT NULL,
    consent_epoch INTEGER NOT NULL,               -- revocation count when admitted
    PRIMARY KEY (batch_id, subject_id)
);

CREATE TABLE IF NOT EXISTS batch_events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    at       TEXT NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT
);

CREATE INDEX IF NOT EXISTS idx_batch_subjects_subject ON batch_subjects(subject_id);
CREATE INDEX IF NOT EXISTS idx_batch_events_batch ON batch_events(batch_id);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextlib.contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
        except BaseException:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
