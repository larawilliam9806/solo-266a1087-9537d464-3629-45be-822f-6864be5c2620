"""Tests for the incremental materialisation and batch-binding fixes."""

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app.db import connect, init_db
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    db_path = tmp_path / "test.sqlite3"
    app = create_app(db_path)
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client, db_path


def _exposure(eid, sid, ts, group):
    return {
        "exposure_id": eid,
        "session_id": sid,
        "occurred_at": ts,
        "group": group,
    }


def test_materialised_first_table_stays_correct(client):
    test_client, _ = client
    # Many later exposures for the same session/group must not change which
    # exposure is the first, including natural-order ties.
    payloads = [
        _exposure("E10", "S1", "2026-01-01T00:00:05Z", "A"),
        _exposure("E2", "S1", "2026-01-01T00:00:01Z", "A"),
        _exposure("E3", "S1", "2026-01-01T00:00:01Z", "A"),  # tie: E2 < E3
        _exposure("E1", "S1", "2026-01-01T00:00:00Z", "A"),
        _exposure("E20", "S1", "2026-01-01T00:00:00Z", "B"),
    ]
    response = test_client.post("/exposures", json=payloads)
    assert response.status_code == 201
    test_client.post(
        "/bindings", json={"session_id": "S1", "account_id": "U1"}
    )
    analysis = test_client.get("/analysis").json()
    assert analysis["excluded_accounts"] == [
        {
            "account_id": "U1",
            "groups": [
                {"group": "A", "first_exposure_id": "E1", "first_exposure_no": "E1"},
                {"group": "B", "first_exposure_id": "E20", "first_exposure_no": "E20"},
            ],
        }
    ]


def test_backfills_derive_only_bound_accounts(client):
    test_client, db_path = client
    for eid, sid in (("E1", "S1"), ("E2", "S1"), ("E3", "S2"), ("E4", "S3")):
        test_client.post(
            "/exposures",
            json=_exposure(eid, sid, f"2026-01-01T00:00:0{eid[1]}Z", "A"),
        )
    test_client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    test_client.post("/bindings", json={"session_id": "S2", "account_id": "U2"})
    # S3 stays unbound: its later exposures must never create an analysis row.
    test_client.post(
        "/exposures", json=_exposure("E9", "S3", "2026-01-02T00:00:00Z", "B")
    )
    rows = connect(db_path).execute(
        "SELECT account_id FROM analysis_samples ORDER BY account_id"
    ).fetchall()
    assert [r["account_id"] for r in rows] == ["U1", "U2"]


def test_batch_binding_returns_per_item_previous_owner_and_status(client):
    test_client, _ = client
    # Existing single bindings first.
    test_client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    test_client.post("/bindings", json={"session_id": "S2", "account_id": "U1"})

    response = test_client.post(
        "/bindings",
        json=[
            {"session_id": "S1", "account_id": "U2"},  # rebound U1 -> U2
            {"session_id": "S2", "account_id": "U2"},  # rebound U1 -> U2
            {"session_id": "S3", "account_id": "U2"},  # fresh bind
            {"session_id": "S4", "account_id": None},  # nothing to unbind
            {"session_id": "S1", "account_id": "U2"},  # unchanged repeat
            {"session_id": "S2", "account_id": None},  # unbind from U1 state
        ],
    )
    assert response.status_code == 200
    results = response.json()["results"]
    assert [r["status"] for r in results] == [
        "rebound",
        "rebound",
        "bound",
        "unchanged",
        "unchanged",
        "unbound",
    ]
    assert [r["previous_account_id"] for r in results] == [
        "U1",
        "U1",
        None,
        None,
        "U2",
        "U2",
    ]
    # Final ownership drives the stored bindings.
    bindings = {
        row["session_id"]: row["account_id"]
        for row in test_client.get("/bindings").json()["bindings"]
    }
    assert bindings == {"S1": "U2", "S3": "U2"}


def test_batch_binding_applies_final_ownership_to_analysis(client):
    test_client, _ = client
    test_client.post(
        "/exposures", json=_exposure("E1", "S1", "2026-01-01T00:00:01Z", "A")
    )
    test_client.post(
        "/exposures", json=_exposure("E2", "S2", "2026-01-01T00:00:02Z", "B")
    )
    test_client.post(
        "/exposures", json=_exposure("E3", "S3", "2026-01-01T00:00:03Z", "A")
    )
    response = test_client.post(
        "/bindings",
        json=[
            {"session_id": "S1", "account_id": "U9"},
            {"session_id": "S2", "account_id": "U9"},
            # Same session again within the batch: the later item wins.
            {"session_id": "S1", "account_id": "U8"},
            {"session_id": "S3", "account_id": "U8"},
        ],
    )
    assert response.status_code == 200
    statuses = [r["status"] for r in response.json()["results"]]
    assert statuses == ["bound", "bound", "rebound", "bound"]
    # U9 keeps only group B (S2); U8 holds A from S1 and S3.
    analysis = test_client.get("/analysis").json()
    samples = {row["account_id"]: row["variant"] for row in analysis["samples"]}
    assert samples == {"U9": "B", "U8": "A"}
    firsts = {row["account_id"]: row["first_exposure_id"] for row in analysis["samples"]}
    assert firsts == {"U9": "E2", "U8": "E1"}


def test_interleaved_backfills_and_bindings_stay_consistent(client):
    test_client, _ = client
    test_client.post(
        "/exposures", json=_exposure("E1", "S1", "2026-01-01T00:00:01Z", "A")
    )
    test_client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    test_client.post(
        "/exposures", json=_exposure("E2", "S2", "2026-01-01T00:00:02Z", "B")
    )
    test_client.post("/bindings", json={"session_id": "S2", "account_id": "U1"})
    # U1 is now cross-group.
    assert len(test_client.get("/analysis").json()["excluded_accounts"]) == 1

    test_client.post("/bindings", json={"session_id": "S2", "account_id": "U2"})
    test_client.post(
        "/exposures", json=_exposure("E3", "S1", "2026-01-01T00:00:03Z", "B")
    )
    # U1 (S1: A and B) is still cross-group with E1 / E3; U2 (S2: B) sampled.
    analysis = test_client.get("/analysis").json()
    excluded = {
        row["account_id"]: [g["group"] for g in row["groups"]]
        for row in analysis["excluded_accounts"]
    }
    assert excluded == {"U1": ["A", "B"]}
    samples = {row["account_id"]: row["first_exposure_id"] for row in analysis["samples"]}
    assert samples == {"U2": "E2"}

    # A full rebuild must reproduce exactly the same result.
    rebuilt = test_client.post("/analysis").json()
    assert rebuilt["samples"] == analysis["samples"]
    assert rebuilt["cross_group"] == analysis["cross_group"]


def test_derivation_row_count_does_not_grow_with_history(client, tmp_path):
    test_client, db_path = client
    test_client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    test_client.post(
        "/exposures", json=_exposure("E1", "S1", "2026-01-01T00:00:00Z", "A")
    )

    def materialised_rows():
        conn = connect(db_path)
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM session_variant_first"
            ).fetchone()[0]
        finally:
            conn.close()

    base = materialised_rows()
    # Pour in many later exposures for an already seen session/variant.
    batch = [
        _exposure(f"E{i}", "S1", f"2026-02-01T00:00:{i:02d}Z", "A")
        for i in range(2, 200)
    ]
    test_client.post("/exposures", json=batch)
    assert materialised_rows() == base  # still one (S1, A) row

    analysis = test_client.get("/analysis").json()
    assert analysis["samples"][0]["first_exposure_id"] == "E1"


def test_batch_binding_write_failure_keeps_original_state(client, tmp_path):
    test_client, db_path = client
    test_client.post(
        "/exposures", json=_exposure("E1", "S1", "2026-01-01T00:00:01Z", "A")
    )
    test_client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    before = test_client.get("/analysis").json()
    before_bindings = test_client.get("/bindings").json()["bindings"]

    raw = sqlite3.connect(db_path)
    raw.execute(
        "CREATE TRIGGER fail_binding BEFORE INSERT ON analysis_samples "
        "BEGIN SELECT RAISE(ABORT, 'forced analysis failure'); END"
    )
    raw.commit()
    raw.close()
    try:
        # Would move S1 to U2 (single-variant -> sample insert); forcing the
        # analysis write to fail must roll back the raw binding change as well.
        response = test_client.post(
            "/bindings",
            json=[
                {"session_id": "S1", "account_id": "U2"},
                {"session_id": "S2", "account_id": "U2"},
            ],
        )
        assert response.status_code == 500
        assert test_client.get("/bindings").json()["bindings"] == before_bindings
        after = test_client.get("/analysis").json()
        assert after["samples"] == before["samples"]
        assert after["generated_at"] == before["generated_at"]
    finally:
        raw = sqlite3.connect(db_path)
        raw.execute("DROP TRIGGER fail_binding")
        raw.commit()
        raw.close()


def test_old_v0_database_is_upgraded_to_match_full_recompute(tmp_path):
    db_path = tmp_path / "legacy.sqlite3"
    # Build a version-0 database with the old schema (no materialised table).
    legacy = connect(db_path)
    legacy.executescript(
        """
        CREATE TABLE exposures (
            exposure_id TEXT PRIMARY KEY,
            session_id  TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            occurred_us INTEGER NOT NULL,
            variant     TEXT NOT NULL
        );
        CREATE TABLE session_bindings (
            session_id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL
        );
        CREATE TABLE analysis_samples (
            account_id TEXT PRIMARY KEY,
            variant TEXT NOT NULL,
            first_exposure_id TEXT NOT NULL
        );
        CREATE TABLE analysis_cross_group (
            account_id TEXT NOT NULL,
            variant TEXT NOT NULL,
            first_exposure_id TEXT NOT NULL,
            PRIMARY KEY (account_id, variant)
        );
        CREATE TABLE analysis_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO analysis_meta VALUES ('updated_at', 'stale');
        """
    )
    rows = [
        ("E10", "S1", "2026-01-01T00:00:00Z", 1_000_000, "A"),
        ("E9", "S1", "2026-01-01T00:00:00Z", 1_000_000, "A"),  # tie -> E9
        ("E2", "S2", "2026-01-01T00:00:02Z", 2_000_000, "B"),
        ("E3", "S3", "2026-01-01T00:00:03Z", 3_000_000, "A"),
    ]
    legacy.executemany(
        "INSERT INTO exposures VALUES (?, ?, ?, ?, ?)", rows
    )
    legacy.executemany(
        "INSERT INTO session_bindings VALUES (?, ?)",
        [("S1", "U1"), ("S2", "U1"), ("S3", "U2")],
    )
    # Deliberately wrong stored analysis to prove the upgrade overwrites it.
    legacy.execute(
        "INSERT INTO analysis_samples VALUES ('U1', 'A', 'WRONG')"
    )
    legacy.commit()
    legacy.close()

    init_db(db_path)  # startup upgrade

    upgraded = connect(db_path)
    try:
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 1
        first = {
            (r["session_id"], r["variant"]): r["exposure_id"]
            for r in upgraded.execute(
                "SELECT session_id, variant, exposure_id FROM session_variant_first"
            )
        }
        assert first == {("S1", "A"): "E9", ("S2", "B"): "E2", ("S3", "A"): "E3"}
        samples = [
            dict(r)
            for r in upgraded.execute(
                "SELECT account_id, variant, first_exposure_id "
                "FROM analysis_samples ORDER BY account_id"
            )
        ]
        assert samples == [
            {"account_id": "U2", "variant": "A", "first_exposure_id": "E3"}
        ]
        cross = [
            dict(r)
            for r in upgraded.execute(
                "SELECT account_id, variant, first_exposure_id "
                "FROM analysis_cross_group ORDER BY account_id, variant"
            )
        ]
        assert cross == [
            {"account_id": "U1", "variant": "A", "first_exposure_id": "E9"},
            {"account_id": "U1", "variant": "B", "first_exposure_id": "E2"},
        ]
        assert upgraded.execute(
            "SELECT value FROM analysis_meta WHERE key = 'updated_at'"
        ).fetchone()["value"] != "stale"
    finally:
        upgraded.close()

    # The running service must serve exactly the upgraded result.
    app = create_app(db_path)
    with TestClient(app, raise_server_exceptions=False) as test_client:
        payload = test_client.get("/analysis").json()
        assert [row["account_id"] for row in payload["samples"]] == ["U2"]
        assert payload["excluded_accounts"][0]["account_id"] == "U1"
        # POST /analysis is idempotent against the upgraded stores.
        rebuilt = test_client.post("/analysis").json()
        assert rebuilt["samples"] == payload["samples"]
        assert rebuilt["cross_group"] == payload["cross_group"]


def test_concurrent_backfill_during_batch_binding_succeeds(tmp_path):
    db_path = tmp_path / "concurrent.sqlite3"
    app = create_app(db_path)
    barrier = threading.Barrier(2)
    errors = []

    def worker():
        try:
            with TestClient(app, raise_server_exceptions=False) as local:
                for sid in ("S1", "S2"):
                    local.post(
                        "/exposures",
                        json=_exposure(
                            f"E-{sid}", sid, "2026-01-01T00:00:01Z", "A"
                        ),
                    )
                barrier.wait()
                # A large batch binding holds the write lock for a while.
                response = local.post(
                    "/bindings",
                    json=[
                        {"session_id": f"S{i}", "account_id": "Ubig"}
                        for i in range(1, 120)
                    ],
                )
                assert response.status_code == 200
        except BaseException as exc:  # pragma: no cover - failure reporting
            errors.append(exc)

    thread = threading.Thread(target=worker)
    with TestClient(app, raise_server_exceptions=False) as test_client:
        thread.start()
        barrier.wait()
        # Ordinary backfill must not lose the lock race: busy_timeout waits.
        response = test_client.post(
            "/exposures",
            json=_exposure("E-back", "S2", "2026-01-01T00:00:05Z", "A"),
        )
        assert response.status_code in (200, 201)
    thread.join()
    assert errors == []
