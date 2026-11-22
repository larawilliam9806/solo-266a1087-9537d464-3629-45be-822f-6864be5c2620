# News Guide A/B Experiment API

FastAPI service that collects guide exposures and session-to-account
bindings for a two-guide news A/B experiment, and maintains the
de-duplicated sample (accounts that saw exactly one variant) plus the
cross-group exclusion list (accounts that saw both variants, with the
first exposure of each variant).

## Run with Docker Compose

```bash
docker compose up --build
```

The API listens on `http://localhost:8000`; the SQLite database lives in
the named volume `experiment-data` at `/data/experiment.sqlite3` (override
with the `DATABASE_PATH` environment variable). Health probe:
`GET /health`. Interactive docs are at `/docs`.

Stop and remove the container (the data volume is preserved):

```bash
docker compose down
```

## Local development

```bash
pip install -r requirements.txt
python -m uvicorn app.main:app --reload
python -m pytest tests/
```

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/exposures` | Record one exposure or a batch (idempotent by `exposure_id`) |
| POST | `/bindings` | Bind/rebind/unbind one session or a batch (`account_id: null` unbinds) |
| PUT | `/bindings/{session_id}` | Bind/rebind one session |
| DELETE | `/bindings/{session_id}` | Unbind one session |
| GET | `/analysis` | Read the stored sample and cross-group rows |
| POST | `/analysis` | Full rebuild / repair from raw exposures and current bindings |
| GET | `/exposures`, `/bindings` | Inspect raw inputs |

## How incremental accounting stays bounded

* `session_variant_first` stores only the earliest exposure per
  session/variant. Each new exposure maintains it with a single indexed
  point lookup, so backfills never re-read a session's history.
* Re-deriving an account reads one row per bound session/variant — the
  amount of data does **not** grow when later exposures are added.
* A batch of binding changes is staged in submission order (each item
  reports the ownership it saw and its `bound`/`rebound`/`unbound`/
  `unchanged` status), then the union of affected old/new accounts is
  re-derived exactly once per account and the stored rows are updated to
  the final ownership.
* Every mutation commits raw inputs and materialised rows in one
  `BEGIN IMMEDIATE` transaction; a write failure rolls everything back.
* `PRAGMA busy_timeout` makes ordinary backfills wait for an in-flight
  batch binding instead of failing on the write lock.
* Databases created before the materialised table (schema version 0) are
  upgraded once at startup: the table is backfilled and the analysis
  result rebuilt, so every stored row matches a full recomputation from
  the raw exposures and current bindings. `POST /analysis` is the manual
  rebuild/repair path and reconciles both stores.

First exposure per variant = earliest UTC timestamp; ties break by
exposure number in natural order (`E9` before `E10`).
