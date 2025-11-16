# backend/app/metrics.py
from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
import redis

router = APIRouter(prefix="/metrics", tags=["metrics"])

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DLQ_NAME = os.getenv("DLQ_NAME", "chrysalis:dlq")

mongo = MongoClient(MONGO_URL)
db = mongo["chrysalis"]

def parse_iso(s):
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

@router.get("/raw_docs_count")
def raw_docs_count():
    try:
        c = db.raw_data.count_documents({})
        return {"count": c}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dlq_count")
def dlq_count():
    try:
        r = redis.from_url(REDIS_URL, decode_responses=False)
        return {"dlq_length": r.llen(DLQ_NAME)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/schema_changes")
def schema_changes(limit: int = 50):
    try:
        docs = list(db.schema_registry.find().sort("version", -1).limit(limit))
        out = []
        for d in docs:
            out.append({
                "version": d.get("version"),
                "created_at": d.get("created_at", None),
                "diff": d.get("diff", None),
            })
        return {"schemas": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/ingest_rate")
def ingest_rate(minutes: int = 60, bucket_mins: int = 1):
    try:
        now = datetime.utcnow()
        start = now - timedelta(minutes=minutes)
        cutoff = start.isoformat()
        cursor = db.raw_data.find({"_ingest_ts": {"$gte": cutoff}}, {"_ingest_ts": 1})
        counts = {}
        for doc in cursor:
            ts = doc.get("_ingest_ts")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                dt = parse_iso(ts)
                if dt is None:
                    continue
            bucket_minute = dt.replace(second=0, microsecond=0) - timedelta(minutes=(dt.minute % bucket_mins))
            key = bucket_minute.isoformat()
            counts[key] = counts.get(key, 0) + 1
        timeline = []
        cur = start.replace(second=0, microsecond=0)
        if cur.minute % bucket_mins != 0:
            cur = cur - timedelta(minutes=(cur.minute % bucket_mins))
        while cur <= now:
            k = cur.isoformat()
            timeline.append({"bucket_start": k, "count": counts.get(k, 0)})
            cur = cur + timedelta(minutes=bucket_mins)
        return {"timeline": timeline}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
