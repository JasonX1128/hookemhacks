# Backend

Small FastAPI scaffold for local development. The current implementation is mock-first, so `/attribute_move` returns deterministic placeholder data that matches the extension's shared contract while the real retrieval and scoring systems are still being wired up.

## Quick start

From the repo root:

```bash
python3 -m pip install -r backend/requirements.txt
python3 -m backend.app
```

For live pipeline refresh work, prefer `python3 -m backend.app` over `uvicorn ... --reload`.
Reload mode watches the repo for changes, and the live all-pages pipeline rewrites artifacts
under `data_pipeline/` continuously. That can force cold backend workers and make the first
`/attribute_move` request much slower than normal.

From inside `backend/`:

```bash
python3 -m pip install -r requirements.txt
python3 -m app
```

## Endpoints

- `GET /health`
- `POST /attribute_move`

Example request:

```bash
curl -X POST http://127.0.0.1:8000/attribute_move \
  -H "Content-Type: application/json" \
  -d '{
    "marketId": "KXINFLATION-CPI-MAY2026-ABOVE35",
    "marketTitle": "Will US CPI YoY print above 3.5% in May 2026?",
    "marketQuestion": "Will the next CPI inflation print come in above 3.5% year-over-year?",
    "clickedTimestamp": "2026-04-18T13:30:00Z",
    "clickedPrice": 0.61,
    "windowStart": "2026-04-18T13:00:00Z",
    "windowEnd": "2026-04-18T14:00:00Z",
    "priceBefore": 0.44,
    "priceAfter": 0.63
  }'
```

## Tests

```bash
python3 -m pytest backend/tests
```

## Pipeline refresh

The extension can trigger a background data-pipeline refresh from the panel, and the backend
exposes start/stop/status endpoints for that workflow. `backend/requirements.txt` now includes
the runtime dependencies needed for that pipeline run as well.

By default, the backend startup runner now prefers the repo virtualenv Python at `.venv/bin/python` when it exists, and falls back to `python3` otherwise. You can still override it with:

```bash
export BACKEND_PIPELINE_STARTUP_PYTHON=/absolute/path/to/python
```

The default refresh config is:

```bash
data_pipeline/configs/kalshi_live_all_pages.json
```

That default config is live-only for metadata discovery:

- it crawls all pages of live Kalshi markets
- it does not supplement discovery with historical market metadata
