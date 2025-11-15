# backend/app/worker.py
"""
Worker: pop jobs from Redis, infer schema, decide on drift, validate docs, write to Mongo.
This worker uses the strict validator and pushes invalid docs to DLQ with reasons.
"""

import os
import time
import orjson
from datetime import datetime
import redis

from .schema_infer import infer_schema_from_sample
from .versioning import get_latest, create_new_version, is_schema_equal
from .storage import StorageManager
from .dlq import send_to_dlq
from .schema_diff import SchemaDriftDetector
from .validator import validate_doc_strict

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = os.getenv("QUEUE_NAME", "chrysalis:ingest:queue")
DLQ_NAME = os.getenv("DLQ_NAME", "chrysalis:dlq")
BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "5"))

r = redis.from_url(REDIS_URL, decode_responses=False)
storage = StorageManager()

def process_job(raw_msg_bytes):
    try:
        job = orjson.loads(raw_msg_bytes)
    except Exception as e:
        print("Invalid job payload:", e)
        send_to_dlq(raw_msg_bytes, reason="invalid_job_payload")
        return

    job_id = job.get("job_id", "unknown")
    docs = job.get("documents", [])
    print(f"[{datetime.utcnow().isoformat()}] Processing job {job_id} with {len(docs)} docs")

    if not isinstance(docs, list) or len(docs) == 0:
        print("Empty or invalid documents in job")
        send_to_dlq(job, reason="empty_documents")
        return

    # Sampling
    sample = docs[:200]
    candidate_schema, field_stats = infer_schema_from_sample(sample)

    # Drift detection & versioning
    latest_meta = get_latest()
    latest_schema = latest_meta["schema"] if latest_meta else None

    diff = SchemaDriftDetector.compute_diff(latest_schema, candidate_schema, field_stats, latest_meta)
    decision = SchemaDriftDetector.decide(diff, sample_size=len(sample), latest_meta=latest_meta)

    schema_version = latest_meta["version"] if latest_meta else 0
    if decision.create_new_version:
        new_meta = create_new_version(candidate_schema, diff, job_id, sample[:5], field_stats)
        schema_version = new_meta["version"]
        print(f"New schema version created: v{schema_version}; reasons: {decision.reasons}")
    else:
        if latest_meta:
            schema_version = latest_meta["version"]
        else:
            new_meta = create_new_version(candidate_schema, diff, job_id, sample[:5], field_stats)
            schema_version = new_meta["version"]
            print(f"Created initial schema version v{schema_version}")

    # Validate each document strictly; failed docs -> DLQ, ok -> buffered for insert
    ok_docs = []
    failed_count = 0
    for doc in docs:
        ok, reason = validate_doc_strict(doc, candidate_schema, field_stats)
        if ok:
            # attach metadata
            doc["_schema_version"] = schema_version
            doc["_ingest_job_id"] = job_id
            doc["_ingest_ts"] = datetime.utcnow().isoformat()
            ok_docs.append(doc)
        else:
            failed_count += 1
            # push failure to DLQ with reason
            send_to_dlq({"doc": doc, "reason": reason, "job_id": job_id}, reason=reason)

    # insert successes
    if ok_docs:
        n = storage.insert_many(ok_docs)
        print(f"Inserted {n} docs into raw_data (schema v{schema_version})")

    if failed_count:
        print(f"Pushed {failed_count} docs to DLQ (job {job_id})")

def main_loop():
    print("Worker started, polling Redis...")
    while True:
        try:
            item = r.brpop(QUEUE_NAME, timeout=BLPOP_TIMEOUT)
            if item:
                _, payload = item
                process_job(payload)
            else:
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("Worker stopping (keyboard interrupt)")
            break
        except Exception as e:
            print("Worker error:", e)
            time.sleep(1)

if __name__ == "__main__":
    main_loop()
