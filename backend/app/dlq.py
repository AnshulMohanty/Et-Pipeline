# backend/app/dlq.py
import redis, os, orjson
from datetime import datetime

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DLQ_NAME = os.getenv("DLQ_NAME", "chrysalis:dlq")
r = redis.from_url(REDIS_URL, decode_responses=False)

def send_to_dlq(payload, reason="unknown"):
    msg = {
        "payload": payload,
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat()
    }
    try:
        r.lpush(DLQ_NAME, orjson.dumps(msg))
    except Exception as e:
        print("DLQ push failed:", e)
