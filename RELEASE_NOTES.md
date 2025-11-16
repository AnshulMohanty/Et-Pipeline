# Release Notes - Phase 11 Finalization

## Changes

### Phase 11 Enhancements

1. **Messy Data Testing**
   - Created comprehensive test corpus (`test_corpus/`) with intentionally messy/unstructured files
   - Supports JSON, CSV, HTML, TXT, and mixed formats
   - Handles non-UTF8 characters with latin1 fallback

2. **Ingest Pipeline Hardening**
   - Enhanced `backend/app/ingest_pipeline.py` with robust format detection
   - Improved error handling and fallback parsing
   - Added support for HTML table extraction and JSON-in-HTML parsing

3. **Validator & Promotion Policy**
   - Added `PROMOTE_BURST` environment variable for demo mode (immediate promotion)
   - Enhanced logging in `decide_promotion()` function
   - Validator validates against latest schema (not candidate)

4. **UI Polish (Streamlit)**
   - Complete UI makeover with three-column layout
   - Latest Schema viewer with expandable JSON
   - Approve Promotion button for manual schema promotion
   - Recent Raw Docs with schema version filtering
   - DLQ viewer with retry functionality (individual and bulk)
   - Post Fixture text area for direct JSON ingestion
   - Health status and real-time metrics in top bar

5. **Backend API Enhancements**
   - Added `/approve` endpoint for manual schema promotion
   - Token-based authentication (PROMOTE_TOKEN env var)

6. **Cleanup**
   - Removed temporary artifacts
   - Organized backup files

## How to Demo (5 Steps)

1. **Start Services**
   ```bash
   cd infra && docker compose up -d
   ```

2. **Access UI**
   - Open `http://localhost:8501` in browser
   - View real-time metrics in top bar

3. **Post Valid Data**
   - Use "Post Fixture" section to paste JSON batch
   - Click "Send to /ingest"
   - Watch schema version increment

4. **Test Validation**
   - Post invalid data (missing required fields)
   - Check DLQ section to see rejected documents
   - View rejection reasons

5. **Manual Promotion & Retry**
   - Use "Approve Promotion" button to manually promote schemas
   - Use "Retry" buttons in DLQ to reprocess failed items

## Environment Variables

### Tunable Variables

- `PROMOTE_PCT` (default: 0.9)`: Field coverage percentage required for automatic schema promotion
- `REQUIRED_PCT` (default: 0.9)`: Historical presence percentage to mark fields as required
- `PROMOTE_BURST` (default: False)`: If True, allows immediate promotion (for demo)
- `PROMOTE_TOKEN` (default: "demo-token")`: Token for manual promotion endpoint

### Connection Variables

- `MONGO_URL`: MongoDB connection string
- `REDIS_URL`: Redis connection string
- `DLQ_NAME`: Dead Letter Queue name
- `QUEUE_NAME`: Ingest queue name
- `API_URL`: Backend API URL (for Streamlit)

## Testing

- Comprehensive fuzz testing with messy data corpus
- All formats (JSON, CSV, HTML, TXT) tested
- Validation and DLQ functionality verified
- Schema promotion policy tested

## Files Changed

- `backend/app/validator.py`: Added PROMOTE_BURST and logging
- `backend/app/ingest_pipeline.py`: Enhanced format detection
- `backend/app/approve.py`: New manual promotion endpoint
- `backend/app/main.py`: Added approve router
- `demo/streamlit_app.py`: Complete UI overhaul
- `cli/ingest_file.py`: Fixed import paths
- `requirements.txt`: Added beautifulsoup4 comment
- `test_corpus/`: New test data directory

## Backup Location

All backups saved to: `tmp_phase11_backup/`

