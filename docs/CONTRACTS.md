# Contracts

The MVP uses one main extension-to-backend contract:

- `GET /health`
- `POST /attribute_move`

## Shared Types

### `MarketClickContext`

```ts
{
  marketId: string;
  marketTitle: string;
  marketQuestion: string;
  clickedTimestamp: string;
  clickedPrice?: number;
  windowStart: string;
  windowEnd: string;
  priceBefore?: number;
  priceAfter?: number;
}
```

### `CatalystCandidate`

```ts
{
  id: string;
  type: "scheduled_event" | "headline" | "platform_signal";
  title: string;
  timestamp: string;
  source: string;
  snippet?: string;
  url?: string;
  semanticScore?: number;
  timeScore?: number;
  importanceScore?: number;
  totalScore?: number;
}
```

### `RelatedMarket`

```ts
{
  marketId: string;
  title: string;
  relationTypes: string[];
  relationStrength: number;
  expectedReactionScore?: number;
  residualZscore?: number;
  status?: "normal" | "possibly_lagging" | "divergent";
  note?: string;
}
```

### `AttributionResponse`

```ts
{
  primaryMarket: MarketClickContext;
  moveSummary: {
    moveMagnitude: number;
    moveDirection: "up" | "down" | "flat";
    jumpScore: number;
  };
  topCatalyst?: CatalystCandidate;
  alternativeCatalysts: CatalystCandidate[];
  confidence: number;
  evidence: CatalystCandidate[];
  relatedMarkets: RelatedMarket[];
}
```

## `GET /health`

Response example:

```json
{
  "status": "ok",
  "service": "market-move-explainer-backend",
  "database": "/absolute/path/to/backend/local_cache.sqlite3"
}
```

## `POST /attribute_move`

Request example:

```json
{
  "marketId": "KXINFLATION-CPI-MAY2026-ABOVE35",
  "marketTitle": "Will US CPI YoY print above 3.5% in May 2026?",
  "marketQuestion": "Will the next CPI inflation print come in above 3.5% year-over-year?",
  "clickedTimestamp": "2026-04-18T13:30:00Z",
  "clickedPrice": 0.61,
  "windowStart": "2026-04-18T13:00:00Z",
  "windowEnd": "2026-04-18T14:00:00Z",
  "priceBefore": 0.44,
  "priceAfter": 0.63
}
```

Response example:

```json
{
  "primaryMarket": {
    "marketId": "KXINFLATION-CPI-MAY2026-ABOVE35",
    "marketTitle": "Will US CPI YoY print above 3.5% in May 2026?",
    "marketQuestion": "Will the next CPI inflation print come in above 3.5% year-over-year?",
    "clickedTimestamp": "2026-04-18T13:30:00Z",
    "clickedPrice": 0.61,
    "windowStart": "2026-04-18T13:00:00Z",
    "windowEnd": "2026-04-18T14:00:00Z",
    "priceBefore": 0.44,
    "priceAfter": 0.63
  },
  "moveSummary": {
    "moveMagnitude": 0.19,
    "moveDirection": "up",
    "jumpScore": 0.95
  },
  "topCatalyst": {
    "id": "headline-cpi-preview-1",
    "type": "headline",
    "title": "Sticky services inflation commentary pushed rate-cut expectations lower",
    "timestamp": "2026-04-18T13:24:00Z",
    "source": "Demo Headlines Fixture",
    "snippet": "Macro desks highlighted stubborn services inflation and a less dovish path for cuts.",
    "totalScore": 0.86
  },
  "alternativeCatalysts": [],
  "confidence": 0.86,
  "evidence": [],
  "relatedMarkets": [
    {
      "marketId": "KXGOLD-ABOVE3400-JUN2026",
      "title": "Will gold trade above $3,400 by June 2026?",
      "relationTypes": ["cross_asset_proxy"],
      "relationStrength": 0.55,
      "expectedReactionScore": 0.58,
      "residualZscore": 2.1,
      "status": "possibly_lagging",
      "note": "Worth checking: precious-metals proxy is lagging the inflation repricing move."
    }
  ]
}
```

## Wording Rules

- Say `likely catalyst`, not a definitive cause.
- Say `related markets`.
- Say `possibly lagging`.
- Say `worth checking`.
- Never claim guaranteed arbitrage or definite causality.

