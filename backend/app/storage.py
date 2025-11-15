# backend/app/storage.py
from pymongo import MongoClient
import os

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client = MongoClient(MONGO_URL)
db = client["chrysalis"]
RAW_COLLECTION = db["raw_data"]

class StorageManager:
    def insert_many(self, docs):
        if not docs:
            return 0
        res = RAW_COLLECTION.insert_many(docs)
        return len(res.inserted_ids)
