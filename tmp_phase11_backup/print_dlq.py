# backend/scripts/print_dlq.py

import os, json, sys

from pymongo import MongoClient

import redis, orjson



REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

DLQ = os.getenv("DLQ_NAME", "chrysalis:dlq")



r = redis.from_url(REDIS_URL, decode_responses=True)



def print_dlq():

    n = r.llen(DLQ)

    print("DLQ length:", n)

    items = r.lrange(DLQ, 0, -1)

    if not items:

        print("DLQ empty.")

        return

    for i, it in enumerate(items):

        try:

            parsed = orjson.loads(it)

            print(f"--- DLQ item {i} ---")

            print(json.dumps(parsed, indent=2, default=str))

        except Exception:

            print("--- DLQ item raw ---")

            print(it)



if __name__ == "__main__":

    print_dlq()
