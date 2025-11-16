# backend/scripts/retry_dlq.py

import os, orjson, redis

REDIS_URL = os.getenv("REDIS_URL","redis://localhost:6379/0")

DLQ = os.getenv("DLQ_NAME","chrysalis:dlq")

QUEUE_NAME = os.getenv("QUEUE_NAME","chrysalis:ingest:queue")

r = redis.from_url(REDIS_URL, decode_responses=True)

def retry_first(n=1):

    for _ in range(n):

        item = r.lpop(DLQ)

        if not item:

            print("DLQ empty")

            return

        # push to ingest queue (right push)

        r.rpush(QUEUE_NAME, item)

        print("Requeued item")

if __name__ == "__main__":

    retry_first(10)

