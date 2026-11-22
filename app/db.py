"""SQLite persistence for the news-guide A/B experiment service.

The schema deliberately separates raw experiment input (``exposures`` and
``session_bindings``) from the materialised analysis result
(``analysis_samples`` and ``analysis_cross_group``).  Every mutation writes
the raw rows and the affected analysis rows inside one ``BEGIN IMMEDIATE``
transaction, so a failed analysis write can never leave the raw input or the
previous result half-updated.

Schema versions (``PRAGMA user_version``)
-----------------------------------------

``1``
    Adds ``session_variant_first``: one row per session/variant holding that
    session's earliest exposure for the variant.  The table is maintained by
    every exposure insert with a single indexed upsert, so re-deriving an
    account reads one row per bound session/variant instead of rescanning the
    whole exposure history of the account.  Databases created at version 0
    are upgraded once at startup, including a full backfill and analysis
    rebuild so the stored result equals a from-scratch recomputation.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS exposures (
    exposure_id TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    occurred_us INTEGER NOT NULL,
    variant     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_exposures_session
    ON exposures (session_id);
CREATE INDEX IF NOT EXISTS idx_exposures_variant
    ON exposures (variant);
-- Supports the per-session/variant first-exposure lookups without a table
-- scan when the materialised table is rebuilt.
CREATE INDEX IF NOT EXISTS idx_exposures_session_variant
    ON exposures (session_id, variant, occurred_us);

CREATE TABLE IF NOT EXISTS session_bindings (
    session_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bindings_account
    ON session_bindings (account_id);

-- Earliest exposure of each session for each variant.  Maintained
-- incrementally on insert; this is the bounded-read source that keeps
-- account derivation independent of the number of stored exposures.
CREATE TABLE IF NOT EXISTS session_variant_first (
    session_id   TEXT NOT NULL,
    variant      TEXT NOT NULL,
    exposure_id  TEXT NOT NULL,
    occurred_us  INTEGER NOT NULL,
    PRIMARY KEY (session_id, variant)
);

-- Accounts that only saw one variant.  One row per account.
CREATE TABLE IF NOT EXISTS analysis_samples (
    account_id       TEXT PRIMARY KEY,
    variant          TEXT NOT NULL,
    first_exposure_id TEXT NOT NULL
);

-- Accounts that saw more than one variant ("cross-group" accounts).  They are
-- kept out of the sample and stored here with their first exposure per group
-- so they can be investigated.  One row per account/variant pair.
CREATE TABLE IF NOT EXISTS analysis_cross_group (
    account_id       TEXT NOT NULL,
    variant          TEXT NOT NULL,
    first_exposure_id TEXT NOT NULL,
    PRIMARY KEY (account_id, variant)
);

CREATE TABLE IF NOT EXISTS analysis_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

DEFAULT_DATABASE_PATH = Path("data") / "experiment.sqlite3"


def database_path_from_env() -> Path:
    raw = (
        os.environ.get("DATABASE_PATH")
        or os.environ.get("DB_PATH")
        or os.environ.get("SQLITE_PATH")
    )
    return Path(raw) if raw else DEFAULT_DATABASE_PATH


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(path),
        isolation_level=None,  # explicit transactions
        timeout=30.0,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(path: Path) -> None:
    """Create or upgrade the database, including one-time data migrations."""

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
        # Imported lazily to avoid an import cycle at module load time.
        from .experiment import upgrade_schema

        upgrade_schema(conn)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialise writers and roll back cleanly on any failure."""

    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def read_snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Read several tables from one consistent SQLite snapshot."""

    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
