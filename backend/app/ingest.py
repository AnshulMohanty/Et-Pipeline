# backend/app/ingest.py
from fastapi import APIRouter, HTTPException, Body
import uuid
import orjson
import redis
import os
from datetime import datetime

router = APIRouter()

# Use localhost because Redis was exposed on host port 6379 by your Docker compose
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_NAME = os.getenv("QUEUE_NAME", "chrysalis:ingest:queue")

# create redis client
r = redis.from_url(REDIS_URL, decode_responses=False)

@router.post("/ingest")
async def ingest(batch: dict = Body(...)):
    """
    Accepts {"source":"...", "documents":[ {...}, {...} ] }
    Returns: {"job_id": "...", "status":"accepted"}
    """
    if "documents" not in batch or not isinstance(batch["documents"], list):
        raise HTTPException(400, detail="Missing 'documents' array in request body")

    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "source": batch.get("source", "unknown"),
        "received_at": datetime.utcnow().isoformat(),
        "documents": batch["documents"]
    }

    # serialize using orjson (bytes)
    msg = orjson.dumps(payload)
    # push onto Redis list (left push)
    r.lpush(QUEUE_NAME, msg)

    return {"job_id": job_id, "status": "accepted"}
