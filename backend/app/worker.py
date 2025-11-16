# backend/app/worker.py

import os, time, orjson

from datetime import datetime

import redis



from .schema_infer import infer_schema_from_sample

from .versioning import get_latest_schema_meta, create_new_version, is_schema_equal

from .storage import StorageManager

from .dlq import send_to_dlq

from .validator import validate_doc_against_schema, decide_promotion



REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

QUEUE_NAME = os.getenv("QUEUE_NAME", "chrysalis:ingest:queue")

BLPOP_TIMEOUT = int(os.getenv("BLPOP_TIMEOUT", "5"))



r = redis.from_url(REDIS_URL, decode_responses=False)

storage = StorageManager()



def compute_simple_diff(old_schema, new_schema, field_stats):

    added, removed, changed = [], [], []

    old_props = old_schema.get("properties", {}) if old_schema else {}

    new_props = new_schema.get("properties", {}) if new_schema else {}

    for k in new_props:

        if k not in old_props:

            added.append(k)

        else:

            if old_props[k] != new_props[k]:

                changed.append(k)

    for k in old_props:

        if k not in new_props:

            removed.append(k)

    return {"added": added, "removed": removed, "changed": changed, "field_stats_sample": field_stats}



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



    # Compare vs latest schema

    latest_meta = get_latest_schema_meta()

    latest_schema = latest_meta["schema"] if latest_meta else None



    diff = compute_simple_diff(latest_schema, candidate_schema, field_stats)



    promote, reasons = decide_promotion(latest_schema, candidate_schema, field_stats)

    schema_version = latest_meta["version"] if latest_meta else 0

    if promote:

        new_meta = create_new_version(candidate_schema, diff, job_id, sample[:5], field_stats)

        schema_version = new_meta["version"]

        print(f"New schema version created: v{schema_version}; reasons: {reasons}")

    else:

        if latest_meta:

            print(f"Candidate schema not promoted; reasons: {reasons}; using latest v{latest_meta['version']}")

        else:

            # Edge case: no latest and not promoted -> promote anyway

            new_meta = create_new_version(candidate_schema, diff, job_id, sample[:5], field_stats)

            schema_version = new_meta["version"]

            print(f"No prior schema; promoted to v{schema_version}")



    # Validate and insert docs using latest schema (not candidate) if latest exists, else candidate

    validation_schema = latest_schema if latest_schema else candidate_schema



    ok_docs = []

    failed = []

    for doc in docs:

        ok, reason = validate_doc_against_schema(doc, validation_schema)

        if ok:

            # attach metadata

            doc["_schema_version"] = schema_version

            doc["_ingest_job_id"] = job_id

            doc["_ingest_ts"] = datetime.utcnow().isoformat()

            ok_docs.append(doc)

        else:

            failed.append({"doc": doc, "reason": reason})



    if ok_docs:

        n = storage.insert_many(ok_docs)

        print(f"Inserted {n} docs into raw_data (schema v{schema_version})")



    if failed:

        for f in failed:

            send_to_dlq(f, reason="validation_failed")

        print(f"Pushed {len(failed)} docs to DLQ")



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
