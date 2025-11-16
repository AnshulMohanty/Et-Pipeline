# Chrysalis — Dynamic ETL Pipeline for Unstructured Data

**Tagline:** Autonomous ETL that infers schemas from messy inputs, versions them, and safely accepts or rejects records with an auditable DLQ.

---

## What this project does (short)

Chrysalis accepts unstructured and semi-structured files (JSON, CSV/TSV, TXT, HTML), infers JSON Schemas from batches, detects schema drift, versions schemas in a `schema_registry`, and stores accepted records in `raw_data` (MongoDB). Records that violate the **current active schema** are routed to a Dead-Letter Queue (Redis) with reasons. The system supports manual promotion of candidate schemas via UI or API and includes a Streamlit demo UI for judges.

---

## Highlights

- Automatic schema inference & versioning

- Deterministic validation policy (validate against latest schema; promote candidate only with coverage rules)

- DLQ with reasons + retry/reprocess support

- Multi-format ingestion: JSON / CSV / TSV / TXT / HTML

- Streamlit demo UI with Latest Schema, Raw Docs, and DLQ views

- Dockerized for quick local demo

---

## Quick architecture (short)

- FastAPI ingestion API → Redis queue → Python worker(s)

- Worker: schema inference (GenSON-style), validator, versioning (Mongo), data storage (Mongo), DLQ (Redis)

- Streamlit UI reads Mongo/Redis for demo

- All services run in Docker Compose (infra/docker-compose.yml)

---

## Quickstart (for judges / local demo)

**Prereqs:**

- Docker Desktop (or Docker) running

- PowerShell (Windows) — commands below are PowerShell-flavored

- Repository checked out locally

**From project root:**

1. **Start services:**

```powershell
cd infra
docker compose up -d --build
cd ..
```

**Health check:**

```powershell
curl.exe -s http://127.0.0.1:8000/health
# expected: {"status":"ok"}
```

**Open the demo UI (default):**

Streamlit: http://127.0.0.1:8501

**Post a test batch (PowerShell, from repo root):**

```powershell
$body = Get-Content -Raw .\fixtures\test_batch.json
Invoke-RestMethod -Uri "http://127.0.0.1:8000/ingest" -Method Post -Body $body -ContentType 'application/json'
```

**Watch worker logs:**

```powershell
docker logs -f chrysalis_worker --tail 120
```

**Inspect DLQ:**

```powershell
docker exec chrysalis_redis redis-cli LLEN chrysalis:dlq
docker exec chrysalis_redis redis-cli LRANGE chrysalis:dlq 0 -1
```

**Inspect schema registry (Mongo):**

```powershell
docker exec chrysalis_mongo mongosh --eval "db.chrysalis.schema_registry.find().sort({version:-1}).limit(5).pretty()"
```

**Demo script (short)**

1. Health & metrics (show /health and Streamlit landing).

2. Ingest a messy file (post CSV/JSON) — show logs creating candidate schema.

3. Show DLQ entries for invalid records (explain reasons).

4. Approve a candidate schema in UI → show schema_registry version increment.

5. Re-run ingest of validated file → show documents appear in raw_data and UI.

(Full step-by-step demo script and files available in tmp_phase11_backup/demo_script_for_judges.md.)

**Important env variables**

- `MONGO_URL` (default: `mongodb://mongo:27017`)

- `REDIS_URL` (default: `redis://redis:6379/0`)

- `PROMOTE_PCT` (default: `0.9`) — coverage required to auto-promote candidate

- `PROMOTE_TOKEN` (demo token if using approve endpoint; default: `demo-token`)

- `DLQ_NAME` (default: `chrysalis:dlq`)

**Production notes & future improvements**

For production, consider using a dedicated Schema Registry service and retention rules in Mongo.

Optionally add Frouros or statistical drift detectors for distribution drift.

For very large files, integrate chunked ingestion with Spark / Delta Lake.

**Project layout (short)**

```bash
backend/        # core Python app (ingest, worker, validator, versioning)
cli/            # small helpers (ingest_file.py)
demo/           # Streamlit UI
infra/          # docker-compose
fixtures/       # test data
scripts/        # smoke tests, utilities
```

**How judges should grade it (suggested rubric)**

- Ingest works for mixed formats (JSON/CSV/TXT/HTML) — pass/fail

- Schema evolution detection and versioning — pass/fail

- DLQ isolating invalid docs with reasons — pass/fail

- UI showing schema and DLQ, and manual promotion — pass/fail

- Clean demo, running locally in Docker — pass/fail

**License & author**

MIT license (add LICENSE file separately if needed)

Author: Anshul (demo project)

