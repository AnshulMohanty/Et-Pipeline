# backend/app/main.py
from fastapi import FastAPI

# import existing routers if present (safe guards)
try:
    from .ingest import router as ingest_router
except Exception:
    ingest_router = None

try:
    from .dlq import router as dlq_router
except Exception:
    dlq_router = None

# import metrics router
from .metrics import router as metrics_router

app = FastAPI(title="Chrysalis ETL API")

if ingest_router:
    app.include_router(ingest_router)
if dlq_router:
    app.include_router(dlq_router)

# include metrics
app.include_router(metrics_router)

@app.get("/health")
def health():
    return {"status": "ok"}
