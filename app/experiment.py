"""Core experiment rules.

Two operations matter:

``bind``
    A session may be bound to exactly one account.  Re-binding replaces the
    account and unbinding removes the row.  After a change only the moved
    session and the old/new accounts are re-derived; other accounts' stored
    analysis rows are left untouched.

``analysis``
    Every bound exposure contributes to its account.  An account that saw a
    single variant is a sample row.  An account that saw more than one
    variant is excluded from the sample and listed separately with its first
    exposure in each variant for investigation.  Within a variant the first
    exposure is the earliest UTC timestamp; ties are broken by exposure
    number order (natural sort, so ``E10`` > ``E9``).
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Sequence

_NATURAL_PARTS = re.compile(r"(\d+)")


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


def derive_account(
    conn: sqlite3.Connection, account_id: str, *, exclude_session_id: str | None = None
) -> None:
    """Recompute one account from its currently bound sessions only.

    ``exclude_session_id`` treats that session as unbound, which is needed
    when an unbind has already removed the row inside the same transaction.
    Only this account's exposures are read; no global rescan takes place.
    """

    sql = (
        "SELECT e.exposure_id, e.variant, e.occurred_us AS occurred_us "
        "FROM session_bindings b JOIN exposures e ON e.session_id = b.session_id "
        "WHERE b.account_id = ?"
    )
    params: list[str] = [account_id]
    if exclude_session_id is not None:
        sql += " AND e.session_id <> ?"
        params.append(exclude_session_id)
    _replace_account_rows(conn, account_id, _first_per_variant(conn.execute(sql, params)))


def bind(conn: sqlite3.Connection, session_id: str, account_id: str | None) -> str:
    """Create, move or remove a session binding and refresh affected accounts.

    Returns ``"unchanged"``, ``"bound"`` or ``"unbound"``.
    """

    previous = conn.execute(
        "SELECT account_id FROM session_bindings WHERE session_id = ?",
        (session_id,),
    ).fetchone()
    previous_account = previous["account_id"] if previous else None

    if account_id is None:
        if previous_account is None:
            return "unchanged"
        conn.execute(
            "DELETE FROM session_bindings WHERE session_id = ?", (session_id,)
        )
        derive_account(conn, previous_account, exclude_session_id=session_id)
        return "unbound"

    if previous_account == account_id:
        return "unchanged"

    conn.execute(
        "INSERT INTO session_bindings (session_id, account_id) VALUES (?, ?) "
        "ON CONFLICT(session_id) DO UPDATE SET account_id = excluded.account_id",
        (session_id, account_id),
    )
    if previous_account is not None:
        derive_account(conn, previous_account)
    derive_account(conn, account_id)
    return "bound"


def compute_analysis(conn: sqlite3.Connection) -> tuple[list[dict], list[dict]]:
    """Compute the full result from bound exposures, without writing it."""

    rows = conn.execute(
        "SELECT b.account_id, e.exposure_id, e.variant, e.occurred_us "
        "FROM session_bindings b JOIN exposures e ON e.session_id = b.session_id"
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

    Must be called inside the caller's transaction; any failure propagates so
    the caller rolls back and the previously stored result survives intact.
    """

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
