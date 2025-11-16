# backend/scripts/print_raw.py
from pymongo import MongoClient
import json
import sys

MONGO_URL = "mongodb://localhost:27017"
client = MongoClient(MONGO_URL)
db = client["chrysalis"]

docs = list(db.raw_data.find().limit(10))
# convert ObjectId and datetime via str
print(json.dumps(docs, default=str, indent=2))
