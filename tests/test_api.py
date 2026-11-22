import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(tmp_path / "test.sqlite3")
    # Match production: an unexpected exception becomes a 500 response rather
    # than propagating into the test.
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


def post_exposure(client, exposure_id, session_id, occurred_at, group):
    return client.post(
        "/exposures",
        json={
            "exposure_id": exposure_id,
            "session_id": session_id,
            "occurred_at": occurred_at,
            "variant": group,
        },
    )


def test_exposure_idempotent_and_conflict(client):
    payload = {
        "exposure_id": "E1",
        "session_id": "S1",
        "occurred_at": "2026-01-01T00:00:00Z",
        "group": "A",
    }
    first = client.post("/exposures", json=payload)
    assert first.status_code == 201
    assert first.json()["status"] == "created"

    replay = client.post("/exposures", json=payload)
    assert replay.status_code == 200
    assert replay.json()["status"] == "existing"

    changed = client.post("/exposures", json={**payload, "group": "B"})
    assert changed.status_code == 409
    assert changed.json()["detail"]["error"] == "exposure_conflict"

    rows = client.get("/exposures").json()["exposures"]
    assert len(rows) == 1
    assert rows[0]["group"] == "A"


def test_analysis_needs_binding_and_lists_first_exposure(client):
    # Exposures arrive before the binding.
    assert post_exposure(client, "E2", "S1", "2026-01-01T00:00:02Z", "A").status_code == 201
    assert post_exposure(client, "E1", "S1", "2026-01-01T00:00:01Z", "A").status_code == 201
    assert client.get("/analysis").json()["samples"] == []

    binding = client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    assert binding.status_code == 200
    assert binding.json()["status"] == "bound"

    analysis = client.get("/analysis").json()
    assert analysis["samples"] == [
        {
            "account_id": "U1",
            "group": "A",
            "variant": "A",
            "first_exposure_id": "E1",
            "first_exposure_no": "E1",
        }
    ]
    assert analysis["cross_group"] == []
    assert analysis["stats"]["bound_exposures"] == 2


def test_same_timestamp_breaks_tie_by_exposure_number(client):
    post_exposure(client, "E10", "S1", "2026-01-01T00:00:00Z", "A")
    post_exposure(client, "E9", "S1", "2026-01-01T00:00:00Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    samples = client.get("/analysis").json()["samples"]
    assert samples[0]["first_exposure_id"] == "E9"


def test_cross_group_account_excluded_and_reported(client):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:01Z", "A")
    post_exposure(client, "E2", "S1", "2026-01-01T00:00:03Z", "A")
    post_exposure(client, "E3", "S2", "2026-01-01T00:00:02Z", "B")
    post_exposure(client, "E4", "S2", "2026-01-01T00:00:04Z", "B")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    client.post("/bindings", json={"session_id": "S2", "account_id": "U1"})

    analysis = client.get("/analysis").json()
    assert analysis["samples"] == []
    assert analysis["excluded_accounts"] == [
        {
            "account_id": "U1",
            "groups": [
                {"group": "A", "first_exposure_id": "E1", "first_exposure_no": "E1"},
                {"group": "B", "first_exposure_id": "E3", "first_exposure_no": "E3"},
            ],
        }
    ]
    assert analysis["stats"]["samples"] == 0
    assert analysis["stats"]["excluded_accounts"] == 1


def test_rebind_and_unbind_recompute_only_affected_accounts(client):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:01Z", "A")
    post_exposure(client, "E2", "S2", "2026-01-01T00:00:02Z", "B")
    post_exposure(client, "E3", "S3", "2026-01-01T00:00:03Z", "A")
    client.post(
        "/bindings",
        json=[
            {"session_id": "S1", "account_id": "U1"},
            {"session_id": "S2", "account_id": "U2"},
            {"session_id": "S3", "account_id": "U3"},
        ],
    )
    assert len(client.get("/analysis").json()["samples"]) == 3

    rebound = client.post("/bindings", json={"session_id": "S1", "account_id": "U2"})
    assert rebound.json()["status"] == "rebound"
    analysis = client.get("/analysis").json()
    # U1 has no exposures left; U2 now spans A and B.
    assert {row["account_id"] for row in analysis["samples"]} == {"U3"}
    assert [row["account_id"] for row in analysis["excluded_accounts"]] == ["U2"]

    removed = client.delete("/bindings/S2")
    assert removed.json()["status"] == "unbound"
    analysis = client.get("/analysis").json()
    # S2's exposures no longer count; U2 is back to group A only via S1.
    assert {row["account_id"] for row in analysis["samples"]} == {"U2", "U3"}
    assert analysis["excluded_accounts"] == []
    assert analysis["stats"]["accounts"] == 2


def test_unbinding_hides_exposures(client):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:01Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    assert len(client.get("/analysis").json()["samples"]) == 1
    client.post("/bindings", json={"session_id": "S1", "account_id": None})
    analysis = client.get("/analysis").json()
    assert analysis["samples"] == []
    assert analysis["stats"]["bound_exposures"] == 0
    # Raw exposure is retained.
    assert client.get("/exposures").json()["count"] == 1


def test_analysis_endpoint_reads_stored_rows(client):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:01Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    before = client.get("/analysis").json()
    assert before["generated_at"] is not None

    rebuilt = client.post("/analysis")
    assert rebuilt.status_code == 200
    assert rebuilt.json()["samples"] == before["samples"]


def test_analysis_write_failure_rolls_back_everything(client, tmp_path):
    post_exposure(client, "E1", "S1", "2026-01-01T00:00:01Z", "A")
    client.post("/bindings", json={"session_id": "S1", "account_id": "U1"})
    before = client.get("/analysis").json()

    db_file = tmp_path / "test.sqlite3"
    raw = sqlite3.connect(db_file)
    raw.execute(
        "CREATE TRIGGER fail_analysis BEFORE INSERT ON analysis_samples "
        "BEGIN SELECT RAISE(ABORT, 'forced analysis failure'); END"
    )
    raw.commit()
    raw.close()

    try:
        response = client.post("/analysis")
        assert response.status_code == 500
        after = client.get("/analysis").json()
        assert after["samples"] == before["samples"]
        assert after["cross_group"] == before["cross_group"]
        assert after["generated_at"] == before["generated_at"]
        assert after["stats"] == before["stats"]
    finally:
        raw = sqlite3.connect(db_file)
        raw.execute("DROP TRIGGER fail_analysis")
        raw.commit()
        raw.close()


def test_batch_exposure_is_atomic_on_conflict(client):
    good = {
        "exposure_id": "E1",
        "session_id": "S1",
        "occurred_at": "2026-01-01T00:00:01Z",
        "group": "A",
    }
    client.post("/exposures", json=good)
    bad_batch = [
        {
            "exposure_id": "E2",
            "session_id": "S1",
            "occurred_at": "2026-01-01T00:00:02Z",
            "group": "A",
        },
        {**good, "group": "B"},
    ]
    response = client.post("/exposures", json=bad_batch)
    assert response.status_code == 409
    rows = client.get("/exposures").json()["exposures"]
    assert [row["exposure_id"] for row in rows] == ["E1"]


def test_aliases_and_integer_ids(client):
    response = client.post(
        "/exposures",
        json={"id": 1, "session": 7, "utc_time": "2026-01-01T00:00:00Z", "group": 2},
    )
    assert response.status_code == 201
    bound = client.post("/session-bindings", json={"session_no": 7, "account": "U1"})
    assert bound.status_code == 200
    sample = client.get("/analysis").json()["samples"][0]
    assert sample["account_id"] == "U1"
    assert sample["group"] == "2"
    assert sample["first_exposure_id"] == "1"
