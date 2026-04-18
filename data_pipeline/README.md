# Macro Preprocessing Pipeline

This package builds a small, high-signal macro market universe for:

- catalyst attribution around CPI, Fed, jobs, GDP, and recession events
- related-market discovery
- propagation and possible lagging analysis

The pipeline is scope-first. It does not scrape or process a broad all-market universe by default.

## Default Macro Run

Default config:

- [macro_default.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/configs/macro_default.json:1)

This scoped run targets:

- `inflation`
- `federal_reserve`
- `monetary_policy`
- `labor_market`
- `jobs`
- `economic_growth`
- `interest_rates`

Primary and secondary keyword seeds included in the config:

- `CPI`
- `inflation`
- `core inflation`
- `FOMC`
- `Fed`
- `rate cut`
- `rate hike`
- `interest rates`
- `unemployment`
- `jobs report`
- `nonfarm payrolls`
- `GDP`
- `recession`
- `PCE`
- `wage growth`
- `labor force participation`
- `treasury yields`
- `economic outlook`

The config keeps the universe bounded:

- `max_markets`: 24
- `per_family_limit`: 5
- `top_k`: 5
- `max_pool_size`: 30

## Commands

Run the full concrete macro ingestion:

```bash
python3 -m data_pipeline.main all --config data_pipeline/configs/macro_default.json --force
```

Run stage by stage:

```bash
python3 -m data_pipeline.fetch_markets --config data_pipeline/configs/macro_default.json --force
python3 -m data_pipeline.fetch_history --config data_pipeline/configs/macro_default.json --force
python3 -m data_pipeline.build_candidates --config data_pipeline/configs/macro_default.json
python3 -m data_pipeline.compute_comovement --config data_pipeline/configs/macro_default.json
python3 -m data_pipeline.compute_cointegration --config data_pipeline/configs/macro_default.json
```

Run the convenience wrapper:

```bash
python3 -m data_pipeline.sample_run --use-default-config --force
```

CLI overrides work on every stage. Example:

```bash
python3 -m data_pipeline.main all \
  --config data_pipeline/configs/macro_default.json \
  --max-markets 20 \
  --per-family-limit 4 \
  --top-k 4 \
  --cross-family-semantic-min 0.55 \
  --force
```

## Output Paths

Artifacts for the default macro run are written to:

- `data_pipeline/artifacts/macro_default/`

Key files:

- [run_scope.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/run_scope.json:1)
- [market_metadata.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/market_metadata.json:1)
- [market_history.csv](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/market_history.csv:1)
- [market_clusters.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/market_clusters.json:1)
- [related_candidates.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/related_candidates.json:1)
- [pair_features.csv](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/pair_features.csv:1)
- [cointegration_metrics.csv](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/cointegration_metrics.csv:1)
- [run_summary.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/run_summary.json:1)
- [artifact_manifest.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/artifacts/macro_default/artifact_manifest.json:1)

Per-market cache files remain provider-scoped under:

- `data_pipeline/cache/mock/macro_default/`

## Artifact Notes

### `market_metadata.json`

Contains only the scoped macro universe selected by the config.

Important fields:

- `category`
- `families`
- `extra.scope_primary_family`
- `extra.matched_scope_families`
- `extra.matched_topic_seeds`

### `related_candidates.json`

Candidate pools are seeded from scoped family overlap and informative terms.

Behavior:

- within-family links are preferred
- cross-family links require strong semantic similarity
- no broad all-pairs generation is performed

### `market_clusters.json`

Clusters are intentionally interpretable topic-family groups rather than one giant connected macro component.

For the default run, this should yield separate clusters such as:

- inflation-related markets
- Fed / monetary policy markets
- labor-market markets
- growth / recession markets
- rate / Treasury markets

### `run_summary.json`

Provides a quick validation view of the scoped run:

- market count
- categories represented
- candidate edge count
- co-movement pair count
- cointegration evaluation count
- cluster summary
- sample related pairs
- top 3 co-movement pairs

## Snapshot Provider

The same config can be used with a local snapshot provider instead of the built-in mock provider:

```bash
python3 -m data_pipeline.main all \
  --provider snapshot \
  --config data_pipeline/configs/macro_default.json \
  --snapshot-dir data_pipeline/fixtures/snapshot \
  --force
```

Expected metadata input:

- `markets.json`
- or `markets.csv`

Expected metadata columns:

- `market_id`
- `ticker`
- `title`
- `question`
- `category`
- optional `families`
- `open_time`
- `close_time`
- `resolution_time`
- `status`
- `tags`

Expected history input:

- `history.csv`
- or `history/<market_id>.csv`

## Current Validation

The current concrete `macro_default` run produced:

- 17 scoped macro markets
- 5 represented categories
- 85 candidate edges
- 85 co-movement feature rows
- 44 pairs that passed the cointegration eligibility filter
- 5 interpretable topic-family clusters

Examples of strong related pairs from the current run:

- `RECESSION-BY-2026-END` <-> `SOFT-LANDING-THROUGH-2026`
- `US10Y-Q2-2026-ABOVE-4_5` <-> `US2Y-Q3-2026-ABOVE-4_75`
- `CORE-CPI-JUN-2026-ABOVE-3_5` <-> `CPI-MAY-2026-HOT`

Examples of top co-movement pairs:

- `GDP-Q4-2026-NEGATIVE` <-> `UNEMPLOYMENT-SEP-2026-ABOVE-4_5`
- `CPI-MAY-2026-HOT` <-> `US2Y-Q3-2026-ABOVE-4_75`
- `GDP-Q4-2026-NEGATIVE` <-> `RECESSION-BY-2026-END`

## Assumptions

- The live provider is still a TODO, so the concrete initial macro run currently uses the curated mock provider.
- The mock universe is intentionally macro-focused and bounded for fast local iteration.
- Cointegration remains optional and conservative.
- Backend integration is intentionally out of scope here.
