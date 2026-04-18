# Macro Preprocessing Pipeline

`data_pipeline/` now supports a conservative manual market discovery and categorization workflow, with `mock`, `snapshot`, and live `kalshi_live` providers.

The policy is:

- ingest broadly
- keep newly discovered markets app-disabled by default
- do not run a continuous agent
- use the LLM only as a periodic batch categorizer
- keep newly proposed categories as candidate categories until separately promoted

## Providers

Available providers:

- `mock`: deterministic local fixture markets for development and tests
- `snapshot`: reads local snapshot files for metadata and history
- `kalshi_live`: reads real market, event, and candlestick data from Kalshi public APIs

The live provider keeps the existing workflow intact:

- real discovery feeds the same metadata and categorization state system
- real history lands in the same normalized cache and artifact format
- promoted-only app exposure still applies
- newly discovered markets still stay app-disabled until they are categorized and promoted

The currently implemented live endpoints are public read endpoints, so no Kalshi API key is required for the commands below.

Recommended live config:

- [kalshi_live_macro.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/configs/kalshi_live_macro.json:1)

## Command Quick Reference

### Pull and inspect discovery state

```bash
python3 -m data_pipeline.manual_categorization pull_markets \
  --config data_pipeline/configs/priority_universe.json
```

What it does:

- fetches discoverable markets
- updates the local market catalog and category registry
- records how many markets are new, unassigned, or pending reevaluation
- keeps newly discovered markets app-disabled by default

### Prepare a categorization batch

```bash
python3 -m data_pipeline.manual_categorization prepare_llm_categorization_batch \
  --config data_pipeline/configs/priority_universe.json
```

What it does:

- checks the configured thresholds
- writes a reviewable batch JSON file
- writes the prompt text that will be sent to the LLM
- exits cleanly without categorization if thresholds are not met

### Run the LLM without applying anything

```bash
python3 -m data_pipeline.manual_categorization run_llm_categorization \
  --config data_pipeline/configs/priority_universe.json
```

What it does:

- reads the prepared batch and prompt from disk
- calls the configured LLM provider
- saves request, raw response, text response, and parsed JSON artifacts
- does not apply assignments or promote categories

### Apply a reviewed response

```bash
python3 -m data_pipeline.manual_categorization apply_llm_categorization \
  --config data_pipeline/configs/priority_universe.json \
  --response-path data_pipeline/artifacts/priority_universe/categorization/llm_runs/<run_id>/response_parsed.json
```

What it does:

- validates referenced market ids and category names
- writes accepted assignments back into local state
- stores new categories as candidate categories only
- does not promote categories

### Evaluate candidate categories

```bash
python3 -m data_pipeline.manual_categorization evaluate_category_promotions \
  --config data_pipeline/configs/priority_universe.json
```

What it does:

- scores candidate categories against promotion thresholds
- reports whether they qualify for promotion
- leaves category state unchanged unless `--promote` is added

### Run the scheduler in safe review mode

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json
```

What it does:

- loops forever
- pulls markets on an interval
- prepares and runs categorization only when thresholds are met
- stops before apply and promotion by default

### Fetch real Kalshi metadata

```bash
python3 -m data_pipeline.main markets \
  --config data_pipeline/configs/kalshi_live_macro.json
```

What it does:

- queries the live Kalshi provider instead of mock data
- writes raw live metadata into `data_pipeline/cache/kalshi_live/<scope>/market_metadata_raw.json`
- writes scoped metadata into `data_pipeline/artifacts/<scope>/market_metadata.json`
- keeps using promoted-only app exposure if manual categorization state already exists

### Fetch real Kalshi history

```bash
python3 -m data_pipeline.main history \
  --config data_pipeline/configs/kalshi_live_macro.json
```

What it does:

- loads the scoped live market set
- fetches real candlestick history for those markets
- caches per-market CSV files locally so reruns are incremental
- writes the combined history artifact used by candidate, comovement, and cointegration steps

### Run the full preprocessing pipeline on live data

```bash
python3 -m data_pipeline.main all \
  --config data_pipeline/configs/kalshi_live_macro.json
```

What it does:

- runs metadata fetch, history fetch, candidate generation, comovement, and cointegration on real Kalshi data
- preserves the same downstream artifact contracts as the mock pipeline
- reuses cached live metadata and history when still fresh

### Run one scheduler cycle for testing

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json \
  --once
```

What it does:

- executes exactly one orchestration cycle
- is useful for testing config, logging, and artifact generation
- exits instead of sleeping forever

### Run the scheduler with auto-apply

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json \
  --auto-apply \
  --auto-evaluate-promotions
```

What it does:

- runs the same loop as safe review mode
- automatically applies parsed LLM output
- evaluates candidate promotions after apply
- still does not auto-promote unless `--auto-promote` is added

## Manual Workflow

### 1. Pull and store discovered markets

This fetches the current discoverable market set, writes it to local artifacts, compares it against known markets, and records which markets are new, still unassigned, or sitting in candidate categories.

```bash
python3 -m data_pipeline.manual_categorization pull_markets \
  --config data_pipeline/configs/priority_universe.json
```

Optional:

- add `--discovery-mode scoped` to limit discovery to the scope config instead of all discoverable markets
- add `--dry-run` to inspect the summary without writing state

To pull from the live Kalshi provider:

```bash
python3 -m data_pipeline.manual_categorization pull_markets \
  --config data_pipeline/configs/kalshi_live_macro.json
```

### 2. Check whether the LLM threshold is met

The pull step writes:

- `data_pipeline/artifacts/<scope>/categorization/discovery_report.json`

That report includes:

- new market count
- known unassigned market count
- candidate-category reevaluation count
- whether the configured LLM threshold was met

Thresholds live in the config under `manual_categorization.llm_thresholds`.

### 3. Prepare a reproducible LLM categorization batch

```bash
python3 -m data_pipeline.manual_categorization prepare_llm_categorization_batch \
  --config data_pipeline/configs/priority_universe.json
```

If thresholds are not met, the command exits cleanly and records a skipped batch status.

If thresholds are met, it writes:

- `pending_llm_categorization_batch.json`
- `pending_llm_categorization_prompt.txt`

Both files live under:

- `data_pipeline/artifacts/<scope>/categorization/`

The batch includes:

- existing promoted categories
- existing candidate categories
- the markets awaiting categorization
- a strict JSON response contract

### 4. Optionally run the LLM from the prepared batch

Put your API key in:

- `data_pipeline/.env`

You can start from:

- [data_pipeline/.env.example](/Users/jasonxie/Documents/hookemhacks/data_pipeline/.env.example:1)

Default provider settings now target Google Gemini:

- provider: `google`
- model: `gemini-2.5-flash`
- API key env var: `GEMINI_API_KEY` or `GOOGLE_API_KEY`

Run the optional execution step:

```bash
python3 -m data_pipeline.manual_categorization run_llm_categorization \
  --config data_pipeline/configs/priority_universe.json
```

This command:

- reads `pending_llm_categorization_batch.json` and `pending_llm_categorization_prompt.txt`
- calls the configured LLM provider with strict JSON schema output
- always writes the request and raw API response to disk
- writes a parsed JSON file only if basic structure validation succeeds
- never applies categorization automatically

Useful overrides:

- config fields under `manual_categorization.llm_execution`
- environment variables such as `GEMINI_API_KEY`, `DATA_PIPELINE_LLM_MODEL`, `DATA_PIPELINE_LLM_TEMPERATURE`, and `DATA_PIPELINE_LLM_MAX_TOKENS`
- `--dry-run` to write the request artifacts without sending an API call

### 5. Review and apply a structured LLM response

Example fixture:

- [manual_categorization_response.sample.json](/Users/jasonxie/Documents/hookemhacks/data_pipeline/fixtures/manual_categorization_response.sample.json:1)

Apply a response:

```bash
python3 -m data_pipeline.manual_categorization apply_llm_categorization \
  --config data_pipeline/configs/priority_universe.json \
  --response-path data_pipeline/fixtures/manual_categorization_response.sample.json
```

Validation rejects malformed output, including:

- unknown market ids
- unknown existing category names
- invalid confidence values
- duplicate market assignment across sections
- proposed new categories below the configured minimum market count
- incomplete coverage of the prepared batch

Accepted results are written back to local state. New categories are stored as `candidate` only and remain app-disabled.

If you used `run_llm_categorization`, pass the parsed response file from the latest run directory into `apply_llm_categorization`.

### 6. Evaluate and optionally promote candidate categories

```bash
python3 -m data_pipeline.manual_categorization evaluate_category_promotions \
  --config data_pipeline/configs/priority_universe.json
```

To actually promote qualifying categories:

```bash
python3 -m data_pipeline.manual_categorization evaluate_category_promotions \
  --config data_pipeline/configs/priority_universe.json \
  --promote
```

Promotion thresholds live under `manual_categorization.promotion_thresholds` and include:

- minimum market count
- minimum average confidence
- minimum coherence
- minimum stability run count

Only promoted categories become app-enabled.

## Persistent Artifacts

The manual workflow writes reviewable state under:

- `data_pipeline/artifacts/<scope>/categorization/`

Key files:

- `market_catalog.json`
- `category_registry.json`
- `market_assignments.json`
- `discovery_report.json`
- `pending_llm_categorization_batch.json`
- `pending_llm_categorization_prompt.txt`
- `llm_runs/`
- `scheduler_runs/`
- `llm_responses/`
- `applied_decisions/`
- `category_promotion_report.json`
- `scheduler_state.json`

## Scheduler Workflow

The scheduler is a thin orchestration layer around the existing commands. It does not replace them.

Default safe review mode:

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json
```

Behavior:

- pull markets every interval
- check whether thresholds are met
- prepare a batch only when needed
- run the LLM only when a new pending batch appears
- stop short of apply and promotion by default

Run the scheduler on live discovery:

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/kalshi_live_macro.json
```

This keeps the same conservative safety boundaries:

- periodic live discovery
- threshold-gated batch preparation
- optional LLM execution
- no auto-apply or auto-promotion unless you explicitly enable those flags

Run forever with auto-apply but no auto-promotion:

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json \
  --auto-apply \
  --auto-evaluate-promotions
```

Run full automation mode including promotion:

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json \
  --auto-apply \
  --auto-evaluate-promotions \
  --auto-promote
```

Run one scheduler cycle for testing:

```bash
python3 -m data_pipeline.manual_categorization run_scheduler \
  --config data_pipeline/configs/priority_universe.json \
  --once
```

Useful scheduler flags:

- `--pull-interval-seconds`
- `--success-cooldown-seconds`
- `--failure-cooldown-seconds`
- `--sleep-jitter-seconds`
- `--max-consecutive-failures`
- `--auto-apply` / `--no-auto-apply`
- `--auto-evaluate-promotions` / `--no-auto-evaluate-promotions`
- `--auto-promote` / `--no-auto-promote`
- `--once`
- `--dry-run`

The scheduler writes per-cycle summaries under:

- `data_pipeline/artifacts/<scope>/categorization/scheduler_runs/`

It also keeps:

- `scheduler_state.json` to remember the last successfully reviewed batch fingerprint
- `scheduler.lock` to avoid duplicate scheduler processes

## Live Provider Notes

The live Kalshi provider is designed to be conservative and resumable rather than trying to ingest all possible history in one shot.

Current behavior:

- metadata discovery paginates through live markets and event indexes
- scoped discovery can use upstream event and series filters when they are selective enough, then applies the existing local scope filter as a final gate
- historical discovery is bounded by Kalshi's `/historical/cutoff` behavior and only supplements scoped market pulls when older settled markets are relevant
- history fetching tries the batch candlestick endpoint first, then falls back to single-market requests when needed
- one bad market or one failed request does not fail the whole history command; stale cache or empty-history fallback is used instead

Current limitations:

- `discovery_mode=all` fetches the discoverable live market universe, not an unbounded crawl of every Kalshi market ever created
- older historical market discovery is intentionally conservative and primarily scoped, so archival backfills remain bounded
- very recently settled markets may still live on the non-historical endpoints until Kalshi's historical cutoff advances

## App-Facing Metadata Behavior

`python3 -m data_pipeline.fetch_markets ...` now prefers the manual categorization state when it exists.

That means:

- only promoted, app-enabled markets are eligible for `market_metadata.json`
- newly discovered unassigned markets stay out of app-facing metadata
- candidate categories stay out of app-facing metadata until promoted

If the manual categorization state does not exist yet, `fetch_markets` falls back to the legacy provider-backed behavior.

## Legacy Compatibility and Migration

The first `pull_markets` run seeds support from existing `market_metadata.json` artifacts where possible.

This does two things:

- preserves currently supported categories and already app-facing markets
- keeps other newly discovered markets conservative and app-disabled by default

The existing preprocessing stages still work:

```bash
python3 -m data_pipeline.main all --config data_pipeline/configs/priority_universe.json --force
```

But the recommended flow is:

1. `pull_markets`
2. `prepare_llm_categorization_batch`
3. `run_llm_categorization` or manually generate a JSON response yourself
4. review the output files in `categorization/llm_runs/`
5. `apply_llm_categorization`
6. `evaluate_category_promotions --promote` when appropriate
7. run the normal preprocessing stages after promoted categories are ready
