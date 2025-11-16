# backend/scripts/print_schemas.py
from pymongo import MongoClient
import json
import sys

MONGO_URL = "mongodb://localhost:27017"
client = MongoClient(MONGO_URL)
db = client["chrysalis"]

schemas = list(db.schema_registry.find().sort("version", -1).limit(10))
print(json.dumps(schemas, default=str, indent=2))
