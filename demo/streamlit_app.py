# demo/streamlit_app.py
import streamlit as st
from pymongo import MongoClient
import os, json
from datetime import datetime

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
client = MongoClient(MONGO_URL)
db = client["chrysalis"]

st.set_page_config(page_title="Project Chrysalis — Demo", layout="wide")
st.title("Project Chrysalis — ETL Demo")

col1, col2 = st.columns([3,1])

with col2:
    st.header("Metrics")
    try:
        raw_count = db.raw_data.count_documents({})
    except Exception:
        raw_count = "n/a"
    try:
        schema_count = db.schema_registry.count_documents({})
    except Exception:
        schema_count = "n/a"
    dlq_len = "n/a"
    try:
        import redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=False)
        dlq_len = r.llen(os.getenv("DLQ_NAME","chrysalis:dlq"))
    except Exception:
        pass

    st.metric("Raw docs", raw_count)
    st.metric("Schema versions", schema_count)
    st.metric("DLQ size", dlq_len)

with col1:
    st.header("Latest schema (most recent)")
    try:
        latest = list(db.schema_registry.find().sort("version", -1).limit(1))
    except Exception:
        latest = []
    if latest:
        # convert ObjectId to str for display
        def fix(o):
            return json.loads(json.dumps(o, default=str))
        st.json(fix(latest[0]))
    else:
        st.info("No schema versions yet.")

st.markdown("---")
st.header("Raw data (latest 50)")
try:
    rows = list(db.raw_data.find().sort([("_id",-1)]).limit(50))
except Exception:
    rows = []
if rows:
    def normalize(d):
        return json.loads(json.dumps(d, default=str))
    display_rows = [normalize(r) for r in rows]
    st.dataframe(display_rows)
else:
    st.info("No raw data yet.")

st.markdown("---")
st.header("DLQ (last 50)")
try:
    import redis, orjson
    r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=False)
    raw_items = r.lrange(os.getenv("DLQ_NAME","chrysalis:dlq"), 0, 49)
    dlq_list = []
    for b in raw_items:
        try:
            dlq_list.append(orjson.loads(b))
        except Exception:
            dlq_list.append({"raw": b.decode() if isinstance(b, (bytes,bytearray)) else str(b)})
    if dlq_list:
        st.dataframe([json.loads(json.dumps(x, default=str)) for x in dlq_list])
    else:
        st.info("DLQ is empty.")
except Exception as e:
    st.warning(f"Cannot read DLQ: {e}")

st.markdown("---")
st.caption(f"Demo last refreshed: {datetime.utcnow().isoformat()} UTC")
