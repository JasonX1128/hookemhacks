# Backend

Small FastAPI scaffold for local development. The current implementation is mock-first, so `/attribute_move` returns deterministic placeholder data that matches the extension's shared contract while the real retrieval and scoring systems are still being wired up.

## Quick start

From the repo root:

```bash
python3 -m pip install -r backend/requirements.txt
python3 -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port 8000
```

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
