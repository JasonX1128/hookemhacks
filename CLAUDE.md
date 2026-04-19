# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kalshify is a hackathon MVP that explains Kalshi prediction market price movements. A Chrome MV3 extension captures chart clicks on Kalshi market pages, sends the context to a local FastAPI backend, which returns catalyst attribution and related market analysis.

## Commands

### Extension (TypeScript)

```bash
npm install                    # Install dependencies
npm run build:extension        # Build extension to extension/dist
npm run watch:extension        # Rebuild on file changes
npm run test:extension         # Run vitest tests
```

### Backend (Python)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r backend/requirements.txt

uvicorn backend.app.main:app --reload    # Start server on :8000
python3 -m pytest backend/tests          # Run tests
```

### Data Pipeline

```bash
# Full macro preprocessing run
python3 -m data_pipeline.main all --config data_pipeline/configs/macro_default.json --force

# Individual stages
python3 -m data_pipeline.fetch_markets --config data_pipeline/configs/macro_default.json --force
python3 -m data_pipeline.fetch_history --config data_pipeline/configs/macro_default.json --force
python3 -m data_pipeline.build_candidates --config data_pipeline/configs/macro_default.json
python3 -m data_pipeline.compute_comovement --config data_pipeline/configs/macro_default.json
python3 -m data_pipeline.compute_cointegration --config data_pipeline/configs/macro_default.json
```

## Architecture

### Extension → Backend Flow

1. Content script (`extension/src/content/`) extracts page metadata and listens for chart clicks
2. Click builds a `MarketClickContext` with marketId, timestamps, and price window
3. Extension POSTs to `http://127.0.0.1:8000/attribute_move`
4. Backend returns `AttributionResponse` with catalyst ranking and related markets
5. Panel (`extension/src/ui/`) renders the result

### Backend Services (`backend/app/services/`)

- `AttributionService`: Orchestrates the full attribution pipeline
- `MoveAnalyzer`: Characterizes price movement (direction, magnitude, jump score)
- `CatalystRetrievalService`: Retrieves candidate catalysts (scheduled events, headlines)
- `CatalystScoringService`: Ranks catalysts by semantic, time, and importance scores
- `RelatedMarketsService`: Finds related markets using preprocessed pipeline artifacts

### Data Pipeline (`data_pipeline/`)

Builds a scoped macro market universe (CPI, Fed, jobs, GDP, recession) with:
- Market metadata and price history
- Related market candidate pairs (family overlap + semantic similarity)
- Co-movement features and cointegration metrics
- Artifacts written to `data_pipeline/artifacts/macro_default/`

### Shared Contracts

`MarketClickContext` and `AttributionResponse` are mirrored in:
- `extension/src/shared/contracts.ts` (TypeScript)
- `backend/app/schemas/contracts.py` (Pydantic)

Type guards in the extension validate API responses at runtime.

## Key Paths

- Extension entry: `extension/src/content/index.ts`, `extension/src/background/index.ts`
- Backend entry: `backend/app/main.py`
- API routes: `backend/app/routes/`
- Mock fixtures: `backend/app/fixtures/`, `extension/src/shared/fixtures/`
- Pipeline config: `data_pipeline/configs/macro_default.json`
- Pipeline artifacts: `data_pipeline/artifacts/macro_default/`

## Development Notes

- Backend runs in mock mode by default (`BACKEND_MOCK_MODE=True`)
- Extension has a dev trigger for deterministic demo flow alongside best-effort live chart capture
- Cointegration is stubbed and only runs for pairs with sufficient history
- The macro scope is intentionally narrow (inflation, Fed, labor, growth, rates)
