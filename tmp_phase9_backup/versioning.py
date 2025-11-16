# backend/app/versioning.py

import os

from datetime import datetime

from pymongo import MongoClient

import orjson



MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

client = MongoClient(MONGO_URL)

db = client.get_database("chrysalis")



_registry = db.schema_registry

# simple incremental integer versioning

def _next_version():

    last = _registry.find_one(sort=[("version", -1)])

    return (last["version"] + 1) if last else 1



def get_latest_schema_meta():

    """Return the latest schema metadata doc or None."""

    doc = _registry.find_one(sort=[("version", -1)])

    return doc



def create_new_version(schema, diff_summary, source_job_id, sample_docs, field_stats):

    """Insert a new schema metadata doc and return it."""

    version = _next_version()

    meta = {

        "version": version,

        "schema": schema,

        "diff": diff_summary,

        "created_at": datetime.utcnow().isoformat(),

        "source_job_id": source_job_id,

        "sample_docs": sample_docs,

        "field_stats": field_stats,

    }

    _registry.insert_one(meta)

    return meta



def is_schema_equal(a, b):

    """Lightweight equality check for schemas (compare JSON serializations)."""

    if a is None and b is None:

        return True

    if (a is None) != (b is None):

        return False

    return orjson.dumps(a) == orjson.dumps(b)
