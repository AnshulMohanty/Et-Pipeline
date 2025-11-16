# backend/app/approve.py
from fastapi import APIRouter, HTTPException, Body, Header
import os
from pymongo import MongoClient
from .versioning import create_new_version, get_latest_schema_meta

router = APIRouter()

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
PROMOTE_TOKEN = os.getenv("PROMOTE_TOKEN", "demo-token")

client = MongoClient(MONGO_URL)
db = client["chrysalis"]

@router.post("/approve")
async def approve_promotion(
    request: dict = Body(...),
    x_token: str = Header(None, alias="X-Token")
):
    """
    Approve manual promotion of a schema.
    Requires token in X-Token header or in request body.
    """
    token = x_token or request.get("token")
    if token != PROMOTE_TOKEN:
        raise HTTPException(401, detail="Invalid token")
    
    schema_id = request.get("schema_id")
    if not schema_id:
        raise HTTPException(400, detail="Missing schema_id")
    
    # Find schema by ID
    try:
        from bson import ObjectId
        schema_doc = db.schema_registry.find_one({"_id": ObjectId(schema_id)})
        if not schema_doc:
            raise HTTPException(404, detail="Schema not found")
        
        # Mark as approved
        db.schema_registry.update_one(
            {"_id": ObjectId(schema_id)},
            {"$set": {"pending_promotion": True, "promoted_at": __import__("datetime").datetime.utcnow().isoformat()}}
        )
        
        return {"status": "approved", "schema_id": schema_id, "version": schema_doc.get("version")}
    except Exception as e:
        raise HTTPException(500, detail=str(e))

