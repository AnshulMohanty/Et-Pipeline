# Chrysalis Demo Script for Judges

## Quick Demo Steps (3-5 minutes)

### 1. Show Health & Status
```bash
curl http://127.0.0.1:8000/health
# Expected: {"status":"ok"}

# Check current metrics
curl http://127.0.0.1:8000/metrics/raw_docs_count
curl http://127.0.0.1:8000/metrics/dlq_count
```

### 2. Ingest Valid Data
```bash
# Post a valid JSON batch
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d @fixtures/test_batch.json

# Check worker logs
docker logs chrysalis_worker --tail 20
# Should show: "Inserted X docs into raw_data"
```

### 3. Show Schema Evolution
```bash
# View latest schema versions
docker exec chrysalis_mongo mongosh chrysalis --quiet \
  --eval "db.schema_registry.find().sort({version:-1}).limit(3).pretty()"

# Show schema promotion decisions in worker logs
docker logs chrysalis_worker | grep "New schema version created"
```

### 4. Demonstrate Validation & DLQ
```bash
# Post invalid data (missing required field)
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d @fixtures/batch_D_missing_required.json

# Post invalid data (type mismatch)
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d @fixtures/batch_E_bad_type.json

# Check DLQ
docker exec chrysalis_worker python backend/scripts/print_dlq.py
# Should show rejected documents with reasons

# Verify invalid docs NOT in raw_data
docker exec chrysalis_mongo mongosh chrysalis --quiet \
  --eval "db.raw_data.find({_ingest_job_id: '...'}).count()"
```

### 5. Show DLQ Retry (Optional)
```bash
# Retry first DLQ item
docker exec chrysalis_worker python backend/scripts/retry_dlq.py

# Check if item reprocessed
docker logs chrysalis_worker --tail 10
```

## Key Features to Highlight

1. **Robust Validation**: Documents validated against latest schema
2. **Schema Evolution**: Automatic schema versioning with promotion policy
3. **DLQ Management**: Invalid documents quarantined with detailed reasons
4. **Multi-format Support**: JSON, CSV, HTML, TXT parsing (ingest_pipeline.py)
5. **Observability**: Metrics endpoints, detailed logging, DLQ inspection

## Demo Flow Summary

1. ✅ Health check → System operational
2. ✅ Ingest valid data → Documents stored, schema created
3. ✅ Schema evolution → New versions when criteria met
4. ✅ Validation → Invalid docs rejected to DLQ
5. ✅ DLQ inspection → See rejection reasons
6. ✅ Retry capability → Reprocess DLQ items if needed

