# demo/streamlit_app.py
import streamlit as st
from pymongo import MongoClient
import os, json
from datetime import datetime, timedelta
import pandas as pd
import redis as redislib

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DLQ_NAME = os.getenv("DLQ_NAME", "chrysalis:dlq")

client = MongoClient(MONGO_URL)
db = client["chrysalis"]

st.set_page_config(page_title="Project Chrysalis — Demo", layout="wide")
st.title("Project Chrysalis — ETL Demo & Metrics")

col1, col2 = st.columns([3,1])

with col2:
    st.header("Realtime Metrics")
    try:
        raw_count = db.raw_data.count_documents({})
    except:
        raw_count = "n/a"
    try:
        schema_count = db.schema_registry.count_documents({})
    except:
        schema_count = "n/a"
    try:
        rconn = redislib.from_url(REDIS_URL, decode_responses=False)
        dlq_len = rconn.llen(DLQ_NAME)
    except:
        dlq_len = "n/a"

    st.metric("Raw docs", raw_count)
    st.metric("Schema versions", schema_count)
    st.metric("DLQ size", dlq_len)

with col1:
    st.header("Latest schema (most recent)")
    try:
        latest = list(db.schema_registry.find().sort("version", -1).limit(1))
    except:
        latest = []

    if latest:
        def safe(o): return json.loads(json.dumps(o, default=str))
        st.json(safe(latest[0]))
    else:
        st.info("No schema versions yet.")

st.markdown("---")
st.header("Ingest rate (timeline)")

minutes = st.slider("Lookback minutes", 10, 1440, 60, 10)
bucket = st.selectbox("Bucket size (mins)", [1,5,10,15,30], index=0)

start = datetime.utcnow() - timedelta(minutes=minutes)
cutoff_iso = start.isoformat()

rows = list(db.raw_data.find({"_ingest_ts": {"$gte": cutoff_iso}}, {"_ingest_ts":1}))

times = []
for r in rows:
    ts = r.get("_ingest_ts")
    if not ts: continue
    try:
        dt = datetime.fromisoformat(ts)
    except:
        try:
            dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f")
        except:
            continue

    times.append(dt.replace(second=0, microsecond=0) - timedelta(minutes=(dt.minute % bucket)))

if times:
    df = pd.DataFrame({"bucket": [t.isoformat() for t in times]})
    counts = df.groupby("bucket").size().reset_index(name="count")

    timeline = []
    cur = start.replace(second=0, microsecond=0)
    if cur.minute % bucket != 0:
        cur = cur - timedelta(minutes=(cur.minute % bucket))

    end = datetime.utcnow()
    idx = []
    while cur <= end:
        idx.append(cur.isoformat())
        cur += timedelta(minutes=bucket)

    idx_df = pd.DataFrame({"bucket": idx})
    merged = idx_df.merge(counts, on="bucket", how="left").fillna(0)

    merged["count"] = merged["count"].astype(int)
    merged["bucket_dt"] = pd.to_datetime(merged["bucket"])
    merged = merged.sort_values("bucket_dt")

    st.line_chart(merged.set_index("bucket_dt")["count"])
else:
    st.info("No ingests found.")

st.markdown("---")
st.header("Schema versions (history)")

try:
    schemas = list(db.schema_registry.find().sort("version", -1).limit(50))
except:
    schemas = []

if schemas:
    df_s = pd.DataFrame([json.loads(json.dumps(s, default=str)) for s in schemas])
    if "version" in df_s.columns:
        st.write(df_s[["version","created_at"]].head(10))
else:
    st.info("No schema history.")

st.markdown("---")
st.header("Raw data (latest 50)")

rows = list(db.raw_data.find().sort([("_id", -1)]).limit(50))
if rows:
    df_raw = pd.DataFrame([json.loads(json.dumps(r, default=str)) for r in rows])
    for c in df_raw.columns:
        df_raw[c] = df_raw[c].apply(lambda x: '' if x is None else str(x))
    st.dataframe(df_raw)
else:
    st.info("No raw data yet.")

st.caption(f"Demo last refreshed: {datetime.utcnow().isoformat()} UTC")
