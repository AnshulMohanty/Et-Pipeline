# backend/app/versioning.py
from pymongo import MongoClient
import os
from datetime import datetime
import pymongo
import json
from dataclasses import is_dataclass, asdict

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client["chrysalis"]

SCHEMA_COLLECTION = db["schema_registry"]

# ensure an index
SCHEMA_COLLECTION.create_index([("version", pymongo.DESCENDING)], unique=False)

def _normalize_diff(diff):
    """
    Ensure the diff is JSON/BSON-serializable (plain dict).
    Accepts dict, dataclass instance, or objects that implement __dict__.
    """
    if diff is None:
        return {}
    if isinstance(diff, dict):
        return diff
    # dataclass -> dict
    if is_dataclass(diff):
        return asdict(diff)
    # fallback to attempt json round-trip on __dict__ or convert via repr
    try:
        return json.loads(json.dumps(diff, default=lambda o: getattr(o, '__dict__', str(o))))
    except Exception:
        # last resort: convert to string representation
        return {"diff_repr": str(diff)}

def get_latest():
    """Return latest schema metadata document or None (as python dict)."""
    doc = SCHEMA_COLLECTION.find_one(sort=[("version", -1)])
    return doc

def create_new_version(schema, diff, cause_batch_id, sample_docs, field_stats=None):
    """
    Create and insert a new schema metadata document.
    'diff' may be a dict or a dataclass instance; we normalize it so Mongo accepts it.
    """
    latest = get_latest()
    new_version = 1 if not latest else latest["version"] + 1

    normalized_diff = _normalize_diff(diff)

    metadata = {
        "version": new_version,
        "schema": schema,
        "diff": normalized_diff,
        "created_at": datetime.utcnow().isoformat(),
        "cause_batch_id": cause_batch_id,
        "sample_docs": sample_docs,
        "field_stats": field_stats or {}
    }
    SCHEMA_COLLECTION.insert_one(metadata)
    return metadata

def is_schema_equal(s1, s2):
    """Simple structural equality check by canonical JSON sorting."""
    try:
        return json.dumps(s1, sort_keys=True) == json.dumps(s2, sort_keys=True)
    except Exception:
        return False
