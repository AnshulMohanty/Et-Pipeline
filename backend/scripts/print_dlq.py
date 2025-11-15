# backend/scripts/print_dlq.py
import redis, orjson, json
r = redis.from_url("redis://localhost:6379/0", decode_responses=False)
items = r.lrange("chrysalis:dlq", 0, -1)
out = []
for b in items:
    try:
        out.append(orjson.loads(b))
    except Exception:
        out.append({"raw": b.decode() if isinstance(b, (bytes, bytearray)) else str(b)})
print(json.dumps(out, indent=2, default=str))
