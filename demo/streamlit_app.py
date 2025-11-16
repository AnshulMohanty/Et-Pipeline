# demo/streamlit_app.py
import streamlit as st
from pymongo import MongoClient
import os, json, requests
from datetime import datetime, timedelta
import pandas as pd
import redis as redislib

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
DLQ_NAME = os.getenv("DLQ_NAME", "chrysalis:dlq")
API_URL = os.getenv("API_URL", "http://backend:8000")
PROMOTE_TOKEN = os.getenv("PROMOTE_TOKEN", "demo-token")

client = MongoClient(MONGO_URL)
db = client["chrysalis"]

st.set_page_config(page_title="Project Chrysalis — Demo", layout="wide", initial_sidebar_state="expanded")

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .footer-text {
        font-size: 0.8rem;
        color: #666;
        margin-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Top bar
st.markdown('<div class="main-header">Project Chrysalis — ETL Demo</div>', unsafe_allow_html=True)

# Quick health & metrics
try:
    health = requests.get(f"{API_URL}/health", timeout=2).json()
    health_status = "🟢 OK" if health.get("status") == "ok" else "🔴 DOWN"
except:
    health_status = "🔴 DOWN"

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

col_top1, col_top2, col_top3, col_top4 = st.columns(4)
with col_top1:
    st.metric("Health", health_status)
with col_top2:
    st.metric("Raw Docs", raw_count)
with col_top3:
    st.metric("Schema Versions", schema_count)
with col_top4:
    st.metric("DLQ Size", dlq_len)

st.markdown("---")

# Main three-column layout
col_left, col_mid, col_right = st.columns([2, 2, 2])

# LEFT COLUMN: Latest Schema
with col_left:
    st.header("📋 Latest Schema")
    try:
        latest = list(db.schema_registry.find().sort("version", -1).limit(1))
        if latest:
            latest_doc = latest[0]
            st.write(f"**Version:** {latest_doc.get('version', 'N/A')}")
            st.write(f"**Created:** {latest_doc.get('created_at', 'N/A')}")
            
            with st.expander("View Schema JSON", expanded=True):
                def safe(o): return json.loads(json.dumps(o, default=str))
                st.json(safe(latest_doc.get('schema', {})))
            
            # Approve Promotion button
            if st.button("✅ Approve Promotion", key="approve_btn", type="primary"):
                try:
                    # Try to call approve endpoint if exists
                    resp = requests.post(
                        f"{API_URL}/approve",
                        json={"schema_id": str(latest_doc.get("_id")), "token": PROMOTE_TOKEN},
                        timeout=5
                    )
                    if resp.status_code == 200:
                        st.success("Promotion approved!")
                    else:
                        st.warning("Approve endpoint not available. Manual promotion via flag.")
                except:
                    # Fallback: write flag to pending_promotions
                    try:
                        db.schema_registry.update_one(
                            {"_id": latest_doc["_id"]},
                            {"$set": {"pending_promotion": True, "promoted_at": datetime.utcnow().isoformat()}}
                        )
                        st.success("Promotion flag set!")
                    except Exception as e:
                        st.error(f"Error: {e}")
        else:
            st.info("No schema versions yet.")
    except Exception as e:
        st.error(f"Error loading schema: {e}")

# MIDDLE COLUMN: Recent Raw Docs
with col_mid:
    st.header("📄 Recent Raw Docs")
    
    schema_filter = st.selectbox(
        "Filter by Schema Version",
        options=["All"] + [str(v) for v in range(1, int(schema_count) + 1) if schema_count != "n/a"],
        key="schema_filter"
    )
    
    try:
        query = {}
        if schema_filter != "All":
            query["_schema_version"] = int(schema_filter)
        
        rows = list(db.raw_data.find(query).sort([("_id", -1)]).limit(20))
        if rows:
            df_raw = pd.DataFrame([json.loads(json.dumps(r, default=str)) for r in rows])
            # Clean up display
            for c in df_raw.columns:
                if c.startswith("_"):
                    continue
                df_raw[c] = df_raw[c].apply(lambda x: str(x)[:50] if x is not None else '')
            
            st.dataframe(df_raw[df_raw.columns[:10]], use_container_width=True, height=400)
            st.caption(f"Showing {len(rows)} documents")
        else:
            st.info("No raw data found.")
    except Exception as e:
        st.error(f"Error loading data: {e}")

# RIGHT COLUMN: DLQ
with col_right:
    st.header("⚠️ DLQ (Dead Letter Queue)")
    
    try:
        rconn_dlq = redislib.from_url(REDIS_URL, decode_responses=True)
        dlq_items_raw = rconn_dlq.lrange(DLQ_NAME, 0, 19)
        
        if dlq_items_raw:
            st.write(f"**Total items:** {len(dlq_items_raw)}")
            
            dlq_items = []
            for i, item in enumerate(dlq_items_raw):
                try:
                    parsed = json.loads(item)
                    dlq_items.append({"index": i, "data": parsed})
                except:
                    dlq_items.append({"index": i, "data": {"raw": item[:100]}})
            
            for item in dlq_items[:10]:
                with st.expander(f"DLQ Item #{item['index']}", expanded=False):
                    reason = item['data'].get('reason', 'unknown')
                    st.write(f"**Reason:** {reason}")
                    st.json(item['data'])
                    
                    if st.button(f"🔄 Retry", key=f"retry_{item['index']}"):
                        try:
                            # Requeue to ingest queue
                            rconn_ingest = redislib.from_url(REDIS_URL, decode_responses=False)
                            rconn_ingest.rpush("chrysalis:ingest:queue", json.dumps(item['data']))
                            st.success("Requeued!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            
            if len(dlq_items) > 10:
                st.caption(f"... and {len(dlq_items) - 10} more items")
            
            # Bulk retry
            if st.button("🔄 Retry All (First 10)", key="retry_all"):
                try:
                    rconn_ingest = redislib.from_url(REDIS_URL, decode_responses=False)
                    for i in range(min(10, len(dlq_items_raw))):
                        rconn_ingest.rpush("chrysalis:ingest:queue", dlq_items_raw[i])
                    st.success(f"Requeued {min(10, len(dlq_items_raw))} items!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.info("DLQ is empty. ✅")
    except Exception as e:
        st.error(f"Error loading DLQ: {e}")

st.markdown("---")

# Post Fixture section
st.header("📤 Post Fixture")
fixture_text = st.text_area("Paste JSON batch here:", height=150, placeholder='{"source": "test", "documents": [{"id": "1", "name": "test"}]}')

if st.button("📨 Send to /ingest", key="post_fixture"):
    if fixture_text:
        try:
            payload = json.loads(fixture_text)
            resp = requests.post(f"{API_URL}/ingest", json=payload, timeout=10)
            if resp.status_code == 200:
                result = resp.json()
                st.success(f"✅ Posted! Job ID: {result.get('job_id', 'N/A')}")
            else:
                st.error(f"Error: {resp.status_code} - {resp.text}")
        except json.JSONDecodeError:
            st.error("Invalid JSON format")
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.warning("Please enter JSON data")

st.markdown("---")

# Footer
st.markdown("""
<div class="footer-text">
<h4>How to Run:</h4>
<ul>
<li>Start services: <code>cd infra && docker compose up -d</code></li>
<li>Access UI: <code>http://localhost:8501</code></li>
<li>API endpoint: <code>http://localhost:8000</code></li>
</ul>

<h4>Demo Steps:</h4>
<ol>
<li>Post a valid JSON batch using the "Post Fixture" section</li>
<li>Watch schema version increment in "Latest Schema"</li>
<li>Post invalid data (missing required fields) to see DLQ populate</li>
<li>Use "Approve Promotion" to manually promote schemas</li>
<li>Use "Retry" buttons to reprocess DLQ items</li>
</ol>
</div>
""", unsafe_allow_html=True)

st.caption(f"Last refreshed: {datetime.utcnow().isoformat()} UTC")
