# Market Move Explainer

Market Move Explainer is a hackathon MVP for Kalshi market pages. A Chrome MV3 extension captures a clicked chart move, sends a typed `MarketClickContext` to a local FastAPI backend, and renders a compact side panel with a likely catalyst, supporting evidence, related markets, and markets worth checking.

## What’s In The Repo

```text
.
├─ extension/         MV3 Chrome extension in TypeScript
├─ backend/           FastAPI backend, fixtures, cache, and tests
├─ data_pipeline/     Curated fixture data plus simple regeneration scripts
├─ docs/              Contracts, task ownership, and demo flow
├─ package.json       Extension build + test scripts
└─ tsconfig.json      TypeScript config for the extension
```

## MVP Shape

- Macro-first scope: economics core plus a small proxy set.
- Conservative language: likely catalyst, related markets, possibly lagging, worth checking.
- Fixture-first retrieval so the demo stays reliable.
- Best-effort real chart click capture plus a dedicated mock path for UI and demo work.

## Local Setup

### 1. Install Node dependencies

```bash
npm install
```

### 2. Build the extension

```bash
npm run build:extension
```

For live rebuilds:

```bash
npm run watch:extension
```

### 3. Create a Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

### 4. Start the backend

From the repo root:

```bash
python3 -m backend.app
```

If you are running the live all-pages pipeline refresh, prefer `python3 -m backend.app` over
`uvicorn ... --reload`. Reload mode watches the repo for file changes, and the live pipeline
continuously rewrites artifacts under `data_pipeline/`, which can cause repeated cold backend
workers and much slower first requests.

### 5. Verify `/health`

```bash
curl http://127.0.0.1:8000/health
```

Expected shape:

```json
{
  "status": "ok",
  "service": "market-move-explainer-backend",
  "database": ".../backend/local_cache.sqlite3"
}
```

### 6. Load the unpacked extension

1. Open `chrome://extensions`.
2. Enable Developer Mode.
3. Click `Load unpacked`.
4. Select `extension/dist`.
5. Open a Kalshi market page.
6. Use either:
   - a real chart click for the best-effort live flow
   - the mock trigger for the deterministic demo flow

## Dev Commands

```bash
npm run build:extension
npm run watch:extension
npm run test:extension
python3 -m pytest backend/tests
python3 data_pipeline/scripts/build_macro_calendar_fixture.py
python3 data_pipeline/scripts/build_headline_fixture.py
python3 data_pipeline/scripts/seed_market_universe.py
```

## End-To-End Flow

1. The user opens a Kalshi market page.
2. The extension extracts page metadata and listens for chart clicks.
3. Clicking the chart builds a `MarketClickContext`.
4. The extension sends the payload to `http://127.0.0.1:8000/attribute_move`.
5. The backend computes move summary, ranks nearby catalysts, and scores related markets.
6. The extension renders the result in a compact panel.

## Fixtures

- Mock request fixture: [backend/app/fixtures/mock_market_click_context.json](/Users/jasonxie/Documents/hookemhacks/backend/app/fixtures/mock_market_click_context.json)
- Mock response fixture: [backend/app/fixtures/mock_attribution_response.json](/Users/jasonxie/Documents/hookemhacks/backend/app/fixtures/mock_attribution_response.json)
- Extension-side fixtures: [extension/src/shared/fixtures](/Users/jasonxie/Documents/hookemhacks/extension/src/shared/fixtures)

## Notes

- The chart extractor is intentionally best-effort and isolated behind small helper modules.
- The backend stays narrow and macro-focused instead of pretending to cover every Kalshi category.
- Cointegration is stubbed and gated so we only revisit it for plausible pairs with enough history.
