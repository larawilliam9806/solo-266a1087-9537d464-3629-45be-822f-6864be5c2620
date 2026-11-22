"""FastAPI application for the two-guide news A/B experiment.

Public surface
--------------
``POST /exposures``          record one exposure or a batch (idempotent)
``POST /bindings``           bind/rebind one or more sessions (null unbinds)
``PUT  /bindings/{id}``      bind/rebind one session
``DELETE /bindings/{id}``    unbind one session
``GET  /analysis``           read the stored sample + cross-group records
``POST /analysis``           rebuild the stored records from bound exposures
``GET  /exposures``          inspect stored exposures
``GET  /bindings``           inspect stored bindings
``GET  /health``             liveness probe

Every mutating request runs in a single SQLite ``BEGIN IMMEDIATE``
transaction that covers both the raw input and the materialised analysis
tables.  A failure while writing the analysis therefore leaves the previously
stored exposures, bindings and analysis result exactly as they were.

The analysis tables are maintained incrementally:

* a new exposure re-derives only the account already bound to its session;
* a binding change re-derives only the moved session's old and new accounts,
  reading just those accounts' bound exposures.

``GET /analysis`` never recomputes; it reads the stored sample and
cross-group rows from one consistent snapshot.  ``POST /analysis`` performs a
full rebuild and is the repair path.
"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from fastapi import Body, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .db import (
    DEFAULT_DATABASE_PATH,
    connect,
    database_path_from_env,
    init_db,
    read_snapshot,
    transaction,
)
from .experiment import (
    ExposureConflict,
    bind,
    derive_account,
    load_analysis,
    persist_analysis,
    save_exposure,
    touch_analysis,
)
from .models import BindingIn, ExposureIn

EXPOSURE_BATCH_LIMIT = 10_000
BINDING_BATCH_LIMIT = 10_000


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _parse_exposures(payload: Any) -> tuple[list[ExposureIn], bool]:
    if isinstance(payload, list):
        if not payload:
            raise HTTPException(status_code=422, detail="exposure batch is empty")
        if len(payload) > EXPOSURE_BATCH_LIMIT:
            raise HTTPException(
                status_code=413,
                detail=f"at most {EXPOSURE_BATCH_LIMIT} exposures per batch",
            )
        return [ExposureIn.model_validate(item) for item in payload], True
    return [ExposureIn.model_validate(payload)], False


def _parse_bindings(payload: Any) -> tuple[list[BindingIn], bool]:
    if isinstance(payload, list):
        if not payload:
            raise HTTPException(status_code=422, detail="binding batch is empty")
        if len(payload) > BINDING_BATCH_LIMIT:
            raise HTTPException(
                status_code=413,
                detail=f"at most {BINDING_BATCH_LIMIT} bindings per batch",
            )
        return [BindingIn.model_validate(item) for item in payload], True
    return [BindingIn.model_validate(payload)], False


def create_app(database_path: str | Path | None = None) -> FastAPI:
    configured_path = Path(database_path) if database_path else None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.database_path = configured_path or database_path_from_env()
        init_db(app.state.database_path)
        yield

    app = FastAPI(
        title="News Guide A/B Experiment",
        version="1.0.0",
        description=(
            "Collects exposures and session-account bindings for the two-guide "
            "news experiment and serves the de-duplicated, cross-group-free sample."
        ),
        lifespan=lifespan,
    )

    def db_path(application: FastAPI) -> Path:
        return getattr(
            application.state,
            "database_path",
            configured_path or DEFAULT_DATABASE_PATH,
        )

    def get_conn(request: Request):
        conn = connect(db_path(request.app))
        try:
            yield conn
        finally:
            conn.close()

    def _ingest_exposures(
        items: Sequence[ExposureIn], conn: sqlite3.Connection
    ) -> tuple[list[dict], int]:
        created = 0
        results: list[dict] = []
        affected: set[str] = set()
        for item in items:
            is_new = save_exposure(conn, item)
            created += int(is_new)
            results.append(
                {
                    "exposure_id": item.exposure_id,
                    "exposure_no": item.exposure_id,
                    "status": "created" if is_new else "existing",
                }
            )
            if is_new:
                row = conn.execute(
                    "SELECT account_id FROM session_bindings WHERE session_id = ?",
                    (item.session_id,),
                ).fetchone()
                if row is not None:
                    affected.add(row["account_id"])
        for account_id in sorted(affected):
            derive_account(conn, account_id)
        if created and affected:
            touch_analysis(conn, utc_now())
        return results, created

    def _apply_binding(conn: sqlite3.Connection, item: BindingIn) -> dict[str, Any]:
        previous = conn.execute(
            "SELECT account_id FROM session_bindings WHERE session_id = ?",
            (item.session_id,),
        ).fetchone()
        previous_account = previous["account_id"] if previous else None
        outcome = bind(conn, item.session_id, item.account_id)
        if item.account_id is None:
            status = "unbound" if outcome == "unbound" else "unchanged"
        elif outcome == "unchanged":
            status = "unchanged"
        elif previous_account is None:
            status = "bound"
        else:
            status = "rebound"
        if outcome != "unchanged":
            touch_analysis(conn, utc_now())
        return {
            "session_id": item.session_id,
            "session_no": item.session_id,
            "account_id": item.account_id,
            "previous_account_id": previous_account,
            "status": status,
        }

    def _analysis_payload(conn: sqlite3.Connection) -> dict[str, Any]:
        stored = load_analysis(conn)
        samples = [
            {
                "account_id": row["account_id"],
                "group": row["variant"],
                "variant": row["variant"],
                "first_exposure_id": row["first_exposure_id"],
                "first_exposure_no": row["first_exposure_id"],
            }
            for row in stored.samples
        ]
        flat_cross = [
            {
                "account_id": row["account_id"],
                "group": row["variant"],
                "variant": row["variant"],
                "first_exposure_id": row["first_exposure_id"],
                "first_exposure_no": row["first_exposure_id"],
            }
            for row in stored.cross_group
        ]
        per_account: dict[str, list[dict]] = {}
        for row in flat_cross:
            per_account.setdefault(row["account_id"], []).append(
                {
                    "group": row["group"],
                    "first_exposure_id": row["first_exposure_id"],
                    "first_exposure_no": row["first_exposure_no"],
                }
            )
        cross_accounts = [
            {"account_id": account_id, "groups": groups}
            for account_id, groups in sorted(per_account.items())
        ]

        exposure_count = conn.execute("SELECT COUNT(*) FROM exposures").fetchone()[0]
        bound_exposure_count = conn.execute(
            "SELECT COUNT(*) FROM exposures e JOIN session_bindings b "
            "ON b.session_id = e.session_id"
        ).fetchone()[0]
        account_count = conn.execute(
            "SELECT COUNT(DISTINCT account_id) FROM session_bindings"
        ).fetchone()[0]

        return {
            "generated_at": stored.updated_at,
            "updated_at": stored.updated_at,
            "samples": samples,
            "sample": samples,
            "cross_group": cross_accounts,
            "excluded_accounts": cross_accounts,
            "cross_group_exposures": flat_cross,
            "stats": {
                "exposures": exposure_count,
                "bound_exposures": bound_exposure_count,
                "accounts": account_count,
                "samples": len(samples),
                "excluded_accounts": len(cross_accounts),
            },
        }

    # ------------------------------------------------------------------ health

    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root() -> dict[str, Any]:
        return {
            "service": "news-guide-ab",
            "endpoints": [
                "POST /exposures",
                "POST /bindings",
                "DELETE /bindings/{session_id}",
                "GET /analysis",
                "POST /analysis",
            ],
        }

    # --------------------------------------------------------------- exposures

    @app.post("/exposures", status_code=201, tags=["exposures"])
    def create_exposure(
        payload: Any = Body(...), conn: sqlite3.Connection = Depends(get_conn)
    ):
        items, is_batch = _parse_exposures(payload)
        try:
            with transaction(conn):
                results, created = _ingest_exposures(items, conn)
        except ExposureConflict as exc:
            existing = conn.execute(
                "SELECT exposure_id, session_id, occurred_at, variant "
                "FROM exposures WHERE exposure_id = ?",
                (exc.exposure_id,),
            ).fetchone()
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "exposure_conflict",
                    "message": "exposure id already exists with different content",
                    "exposure_id": exc.exposure_id,
                    "stored": dict(existing) if existing else None,
                },
            ) from exc

        if is_batch:
            return JSONResponse(
                status_code=201 if created else 200,
                content={"accepted": created, "results": results},
            )
        item = results[0]
        return JSONResponse(
            status_code=201 if item["status"] == "created" else 200,
            content=item,
        )

    @app.get("/exposures", tags=["exposures"])
    def list_exposures(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
        with read_snapshot(conn):
            rows = [
                {
                    "exposure_id": row["exposure_id"],
                    "exposure_no": row["exposure_id"],
                    "session_id": row["session_id"],
                    "session_no": row["session_id"],
                    "occurred_at": row["occurred_at"],
                    "group": row["variant"],
                    "variant": row["variant"],
                    "account_id": row["account_id"],
                }
                for row in conn.execute(
                    "SELECT e.exposure_id, e.session_id, e.occurred_at, e.variant, "
                    "b.account_id "
                    "FROM exposures e "
                    "LEFT JOIN session_bindings b ON b.session_id = e.session_id "
                    "ORDER BY e.occurred_us, e.exposure_id"
                ).fetchall()
            ]
        return {"exposures": rows, "count": len(rows)}

    # ---------------------------------------------------------------- bindings

    @app.post("/bindings", tags=["bindings"])
    def create_bindings(
        payload: Any = Body(...), conn: sqlite3.Connection = Depends(get_conn)
    ):
        items, is_batch = _parse_bindings(payload)
        with transaction(conn):
            results = [_apply_binding(conn, item) for item in items]
        if is_batch:
            return {"results": results}
        return JSONResponse(status_code=200, content=results[0])

    @app.put("/bindings/{session_id}", tags=["bindings"])
    def put_binding(
        session_id: str,
        payload: Any = Body(default=None),
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> dict[str, Any]:
        if isinstance(payload, dict):
            body = {**payload, "session_id": session_id}
        else:
            body = {"session_id": session_id, "account_id": payload}
        item = BindingIn.model_validate(body)
        with transaction(conn):
            return _apply_binding(conn, item)

    @app.delete("/bindings/{session_id}", tags=["bindings"])
    def delete_binding(
        session_id: str,
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> dict[str, Any]:
        item = BindingIn.model_validate({"session_id": session_id, "account_id": None})
        with transaction(conn):
            return _apply_binding(conn, item)

    @app.get("/bindings", tags=["bindings"])
    def list_bindings(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
        with read_snapshot(conn):
            rows = [
                {
                    "session_id": row["session_id"],
                    "session_no": row["session_id"],
                    "account_id": row["account_id"],
                }
                for row in conn.execute(
                    "SELECT session_id, account_id FROM session_bindings "
                    "ORDER BY session_id"
                ).fetchall()
            ]
        return {"bindings": rows, "count": len(rows)}

    # ---------------------------------------------------------------- analysis

    @app.get("/analysis", tags=["analysis"])
    def get_analysis(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
        with read_snapshot(conn):
            return _analysis_payload(conn)

    @app.post("/analysis", tags=["analysis"])
    def run_analysis(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
        # Rebuild inside one transaction: either both the new sample and
        # cross-group rows are stored, or the previously stored exposures,
        # bindings and result remain untouched.
        with transaction(conn):
            persist_analysis(conn, utc_now())
        with read_snapshot(conn):
            return _analysis_payload(conn)

    @app.get("/analysis/samples", tags=["analysis"])
    def get_samples(conn: sqlite3.Connection = Depends(get_conn)) -> dict[str, Any]:
        with read_snapshot(conn):
            payload = _analysis_payload(conn)
        return {"samples": payload["samples"], "count": len(payload["samples"])}

    @app.get("/analysis/cross-group", tags=["analysis"])
    def get_cross_group(
        conn: sqlite3.Connection = Depends(get_conn),
    ) -> dict[str, Any]:
        with read_snapshot(conn):
            payload = _analysis_payload(conn)
        return {
            "cross_group": payload["cross_group"],
            "excluded_accounts": payload["excluded_accounts"],
            "count": len(payload["cross_group"]),
        }

    # Compatibility aliases for integrations that use longer resource names.
    for path, endpoint, methods in (
        ("/session-bindings", create_bindings, ["POST"]),
        ("/exposures/batch", create_exposure, ["POST"]),
        ("/analysis/recompute", run_analysis, ["POST"]),
        ("/analysis/results", get_analysis, ["GET"]),
        ("/sample", get_analysis, ["GET"]),
    ):
        app.add_api_route(path, endpoint, methods=methods, include_in_schema=False)

    return app


app = create_app()
