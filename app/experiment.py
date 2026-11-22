"""Core experiment rules.

Two operations matter:

``bind``
    A session may be bound to exactly one account.  Re-binding replaces the
    account and unbinding removes the row.  After a change only the moved
    session's old and new accounts are re-derived; other accounts' stored
    analysis rows are left untouched.  Batch binding applies every raw change
    first and re-derives the union of affected accounts exactly once, so an
    account that receives many sessions in one batch is never re-read once
    per session.

``analysis``
    Every bound exposure contributes to its account.  An account that saw a
    single variant is a sample row.  An account that saw more than one
    variant is excluded from the sample and listed separately with its first
    exposure in each variant for investigation.  Within a variant the first
    exposure is the earliest UTC timestamp; ties are broken by exposure
    number order (natural sort, so ``E10`` > ``E9``).

Bounded derivation
------------------

``session_variant_first`` stores, per session/variant, only the earliest
exposure.  Each new exposure maintains it with one indexed point lookup, and
account derivation reads one row per bound session/variant.  Adding later
exposures for an already seen session/variant therefore never increases the
amount of data an account derivation has to read: the cost does not grow
linearly with the number of stored exposure rows.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

from .db import SCHEMA_VERSION

_NATURAL_PARTS = re.compile(r"(\d+)")


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def natural_key(value: str) -> tuple:
    """Natural ordering key: ``E9`` sorts before ``E10``."""

    parts: list[tuple[int, object]] = []
    for part in _NATURAL_PARTS.split(value):
        if not part:
            continue
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part))
    return (tuple(parts), value)


def exposure_sort_key(exposure_id: str, occurred_us: int) -> tuple:
    """Deterministic ordering: UTC timestamp first, then exposure number."""

    return (occurred_us, natural_key(exposure_id))


class ExposureConflict(Exception):
    """Same exposure id re-uploaded with different content."""

    def __init__(self, exposure_id: str) -> None:
        super().__init__(exposure_id)
        self.exposure_id = exposure_id


def _record_session_first(conn: sqlite3.Connection, exposure) -> None:
    """Maintain the earliest exposure per session/variant in one point lookup.

    Only the primary-key index of ``session_variant_first`` is touched, so the
    cost is independent of how many exposures the session already has.
    """

    stored = conn.execute(
        "SELECT exposure_id, occurred_us FROM session_variant_first "
        "WHERE session_id = ? AND variant = ?",
        (exposure.session_id, exposure.variant),
    ).fetchone()
    if stored is None:
        conn.execute(
            "INSERT INTO session_variant_first "
            "(session_id, variant, exposure_id, occurred_us) "
            "VALUES (?, ?, ?, ?)",
            (
                exposure.session_id,
                exposure.variant,
                exposure.exposure_id,
                exposure.occurred_us,
            ),
        )
        return
    incoming = exposure_sort_key(exposure.exposure_id, exposure.occurred_us)
    current = exposure_sort_key(stored["exposure_id"], stored["occurred_us"])
    if incoming < current:
        conn.execute(
            "UPDATE session_variant_first SET exposure_id = ?, occurred_us = ? "
            "WHERE session_id = ? AND variant = ?",
            (
                exposure.exposure_id,
                exposure.occurred_us,
                exposure.session_id,
                exposure.variant,
            ),
        )


def save_exposure(conn: sqlite3.Connection, exposure) -> bool:
    """Insert an exposure; return ``True`` if new, ``False`` for a replay.

    A replay is only accepted when every field matches the stored row; a
    conflicting replay raises :class:`ExposureConflict`.
    """

    existing = conn.execute(
        "SELECT session_id, occurred_at, variant "
        "FROM exposures WHERE exposure_id = ?",
        (exposure.exposure_id,),
    ).fetchone()
    if existing is not None:
        if (
            existing["session_id"] != exposure.session_id
            or existing["occurred_at"] != exposure.occurred_at
            or existing["variant"] != exposure.variant
        ):
            raise ExposureConflict(exposure.exposure_id)
        return False

    conn.execute(
        "INSERT INTO exposures "
        "(exposure_id, session_id, occurred_at, occurred_us, variant) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            exposure.exposure_id,
            exposure.session_id,
            exposure.occurred_at,
            exposure.occurred_us,
            exposure.variant,
        ),
    )
    _record_session_first(conn, exposure)
    return True


def _first_per_variant(rows: Iterable[sqlite3.Row]) -> list[tuple[str, str]]:
    """Collapse exposures to the earliest per variant (sorted by variant)."""

    best: dict[str, tuple[tuple, str]] = {}
    for row in rows:
        variant = row["variant"]
        key = exposure_sort_key(row["exposure_id"], row["occurred_us"])
        if variant not in best or key < best[variant][0]:
            best[variant] = (key, row["exposure_id"])
    return sorted((variant, value[1]) for variant, value in best.items())


def _replace_account_rows(
    conn: sqlite3.Connection, account_id: str, per_variant: Sequence[tuple[str, str]]
) -> None:
    """Replace the stored analysis rows for one account.

    One variant -> sample row; several variants -> cross-group rows; none ->
    the account disappears from both tables.
    """

    conn.execute("DELETE FROM analysis_samples WHERE account_id = ?", (account_id,))
    conn.execute(
        "DELETE FROM analysis_cross_group WHERE account_id = ?", (account_id,)
    )
    if not per_variant:
        return
    if len(per_variant) == 1:
        variant, exposure_id = per_variant[0]
        conn.execute(
            "INSERT INTO analysis_samples (account_id, variant, first_exposure_id) "
            "VALUES (?, ?, ?)",
            (account_id, variant, exposure_id),
        )
    else:
        conn.executemany(
            "INSERT INTO analysis_cross_group "
            "(account_id, variant, first_exposure_id) VALUES (?, ?, ?)",
            [
                (account_id, variant, exposure_id)
                for variant, exposure_id in per_variant
            ],
        )


def derive_account(conn: sqlite3.Connection, account_id: str) -> None:
    """Recompute one account from its currently bound sessions only.

    Only the per-session/variant first-exposure rows are read: one row per
    bound session/variant, regardless of how many individual exposures those
    sessions accumulated.  No global rescan takes place.
    """

    rows = conn.execute(
        "SELECT f.variant AS variant, f.exposure_id AS exposure_id, "
        "f.occurred_us AS occurred_us "
        "FROM session_bindings b "
        "JOIN session_variant_first f ON f.session_id = b.session_id "
        "WHERE b.account_id = ?",
        (account_id,),
    ).fetchall()
    _replace_account_rows(conn, account_id, _first_per_variant(rows))


@dataclass(frozen=True)
class BindingChange:
    """Raw outcome of one binding item inside a batch."""

    session_id: str
    account_id: str | None
    previous_account_id: str | None
    changed: bool


def stage_binding(
    conn: sqlite3.Connection, session_id: str, account_id: str | None
) -> BindingChange:
    """Write one binding change to the raw table without re-deriving anything.

    The caller collects the union of affected accounts and re-derives each of
    them once, which keeps a batch that moves many sessions onto the same
    account from re-reading that account's history once per item.
    """

    previous = conn.execute(
        "SELECT account_id FROM session_bindings WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    previous_account = previous["account_id"] if previous else None

    if account_id is None:
        if previous_account is None:
            return BindingChange(session_id, None, None, False)
        conn.execute(
            "DELETE FROM session_bindings WHERE session_id = ?", (session_id,)
        )
        return BindingChange(session_id, None, previous_account, True)

    if previous_account == account_id:
        return BindingChange(session_id, account_id, previous_account, False)

    conn.execute(
        "INSERT INTO session_bindings (session_id, account_id) VALUES (?, ?) "
        "ON CONFLICT(session_id) DO UPDATE SET account_id = excluded.account_id",
        (session_id, account_id),
    )
    return BindingChange(session_id, account_id, previous_account, True)


def derive_affected(
    conn: sqlite3.Connection, changes: Iterable[BindingChange]
) -> set[str]:
    """Re-derive every account touched by a set of binding changes, once each.

    Both the account a session left and the account it joined are included.
    Sessions unbound at the end simply no longer appear in the remaining
    accounts' joins.
    """

    affected: set[str] = set()
    for change in changes:
        if not change.changed:
            continue
        if change.previous_account_id is not None:
            affected.add(change.previous_account_id)
        if change.account_id is not None:
            affected.add(change.account_id)
    for account_id in sorted(affected):
        derive_account(conn, account_id)
    return affected


def bind(conn: sqlite3.Connection, session_id: str, account_id: str | None) -> str:
    """Create, move or remove a session binding and refresh affected accounts.

    Returns ``"unchanged"``, ``"bound"`` or ``"unbound"``.
    """

    change = stage_binding(conn, session_id, account_id)
    if not change.changed:
        return "unchanged"
    derive_affected(conn, [change])
    return "unbound" if account_id is None else "bound"


def reconcile_session_variant_first(conn: sqlite3.Connection) -> None:
    """Rebuild the per-session/variant first-exposure table from raw rows.

    This is the repair path used by schema upgrade and the manual analysis
    rebuild; afterwards the materialised table is provably identical to a
    full recomputation from the raw exposures, including natural-order tie
    breaks on identical timestamps.
    """

    rows = conn.execute(
        "SELECT session_id, variant, exposure_id, occurred_us FROM exposures"
    ).fetchall()
    best: dict[tuple[str, str], tuple[tuple, str, int]] = {}
    for row in rows:
        key = (row["session_id"], row["variant"])
        candidate = exposure_sort_key(row["exposure_id"], row["occurred_us"])
        if key not in best or candidate < best[key][0]:
            best[key] = (candidate, row["exposure_id"], row["occurred_us"])

    conn.execute("DELETE FROM session_variant_first")
    conn.executemany(
        "INSERT INTO session_variant_first "
        "(session_id, variant, exposure_id, occurred_us) VALUES (?, ?, ?, ?)",
        [
            (session_id, variant, exposure_id, occurred_us)
            for (session_id, variant), (
                _key,
                exposure_id,
                occurred_us,
            ) in best.items()
        ],
    )


def compute_analysis(conn: sqlite3.Connection) -> tuple[list[dict], list[dict]]:
    """Compute the full result from bound exposures, without writing it.

    Reads the maintained per-session/variant first-exposure table, which the
    rebuild path reconciles from raw exposures first, so the output always
    matches a from-scratch computation over the raw data and current bindings.
    """

    rows = conn.execute(
        "SELECT b.account_id AS account_id, f.exposure_id AS exposure_id, "
        "f.variant AS variant, f.occurred_us AS occurred_us "
        "FROM session_bindings b "
        "JOIN session_variant_first f ON f.session_id = b.session_id"
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row["account_id"], []).append(row)

    samples: list[dict] = []
    cross: list[dict] = []
    for account_id in sorted(grouped):
        per_variant = _first_per_variant(grouped[account_id])
        if len(per_variant) == 1:
            variant, exposure_id = per_variant[0]
            samples.append(
                {
                    "account_id": account_id,
                    "variant": variant,
                    "first_exposure_id": exposure_id,
                }
            )
        else:
            for variant, exposure_id in per_variant:
                cross.append(
                    {
                        "account_id": account_id,
                        "variant": variant,
                        "first_exposure_id": exposure_id,
                    }
                )
    return samples, cross


def persist_analysis(conn: sqlite3.Connection, updated_at: str) -> None:
    """Replace the stored analysis result with a freshly computed one.

    The materialised first-exposure table is reconciled from the raw rows
    first, so a manual rebuild repairs both stores at once.  Must be called
    inside the caller's transaction; any failure propagates so the caller
    rolls back and the previously stored result survives intact.
    """

    reconcile_session_variant_first(conn)
    samples, cross = compute_analysis(conn)
    write_analysis(conn, samples, cross, updated_at)


def write_analysis(
    conn: sqlite3.Connection,
    samples: Sequence[dict],
    cross: Sequence[dict],
    updated_at: str,
) -> None:
    """Atomically replace the stored sample and cross-group rows.

    Callers must supply the rows inside an open transaction; the raw input
    tables are never touched here.
    """

    conn.execute("DELETE FROM analysis_samples")
    conn.execute("DELETE FROM analysis_cross_group")
    conn.executemany(
        "INSERT INTO analysis_samples (account_id, variant, first_exposure_id) "
        "VALUES (:account_id, :variant, :first_exposure_id)",
        samples,
    )
    conn.executemany(
        "INSERT INTO analysis_cross_group (account_id, variant, first_exposure_id) "
        "VALUES (:account_id, :variant, :first_exposure_id)",
        cross,
    )
    touch_analysis(conn, updated_at)


def touch_analysis(conn: sqlite3.Connection, updated_at: str) -> None:
    """Record that the stored result was refreshed within this transaction."""

    conn.execute(
        "INSERT INTO analysis_meta (key, value) VALUES ('updated_at', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (updated_at,),
    )


def upgrade_schema(conn: sqlite3.Connection) -> None:
    """Upgrade an older database in place, fully inside one transaction.

    A version-0 database (created before the materialised table existed) gets
    its ``session_variant_first`` backfilled from raw exposures and its
    analysis result rebuilt from raw exposures plus current bindings, so after
    startup every stored row matches a full recomputation.  Any failure rolls
    the whole upgrade back and leaves the original database untouched.
    """

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version >= SCHEMA_VERSION:
        return

    conn.execute("BEGIN IMMEDIATE")
    try:
        if version < 1:
            reconcile_session_variant_first(conn)
            # Only rebuild the stored result when the database already holds
            # experiment data; a brand-new empty database stays ungenerated
            # until its first real write.
            has_data = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM exposures LIMIT 1)"
            ).fetchone()[0]
            if has_data:
                samples, cross = compute_analysis(conn)
                write_analysis(conn, samples, cross, utc_now())
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


@dataclass(frozen=True)
class StoredAnalysis:
    samples: list[dict]
    cross_group: list[dict]
    updated_at: str | None

    @property
    def generated(self) -> bool:
        return self.updated_at is not None


def load_analysis(conn: sqlite3.Connection) -> StoredAnalysis:
    """Read the stored sample and cross-group rows from one snapshot."""

    samples = [
        dict(row)
        for row in conn.execute(
            "SELECT account_id, variant, first_exposure_id FROM analysis_samples "
            "ORDER BY account_id"
        ).fetchall()
    ]
    cross = [
        dict(row)
        for row in conn.execute(
            "SELECT account_id, variant, first_exposure_id "
            "FROM analysis_cross_group ORDER BY account_id, variant"
        ).fetchall()
    ]
    meta = conn.execute(
        "SELECT value FROM analysis_meta WHERE key = 'updated_at'"
    ).fetchone()
    return StoredAnalysis(
        samples=samples,
        cross_group=cross,
        updated_at=meta["value"] if meta else None,
    )
