# Demo Script

## Goal

Show that a developer can run the backend locally, load the extension, click a Kalshi market chart, and see a plausible explanation with evidence, related markets, and at least one worth-checking signal.

## Setup

1. Run `npm run build:extension`.
2. Start the backend with `python3 -m backend.app`.
3. Verify `curl http://127.0.0.1:8000/health`.
4. Load `extension/dist` as an unpacked extension in Chrome.

## Preferred Demo: Real Chart Click

1. Open a Kalshi macro market page.
2. Mention that the mock path exists, but start with a real chart click.
3. Click a visible point on the chart.
4. Narrate the response:
   - market context and approximate click timestamp were extracted
   - the backend ranked nearby catalysts
   - related markets and a worth-checking dislocation were surfaced
5. Highlight the wording:
   - likely catalyst
   - related markets
   - possibly lagging
   - worth checking

## Reliable Fallback Demo

1. Use the extension’s mock action.
2. Show that the panel still renders end to end even if live extraction is weak on a specific page.
3. Call out that this keeps UI and backend work unblocked during the hackathon.

## Talking Points

- This is an event attribution and propagation assistant, not a causality oracle.
- The first version is intentionally macro-focused.
- Fixture-first retrieval makes the demo deterministic.
- Cointegration is optional and deliberately gated behind a stub.

## Failure Handling

- If `/health` fails, restart the backend process.
- If a page’s chart DOM has shifted, switch to the mock flow immediately.
- If a result looks thin, explain that the system is conservative and best-effort rather than overstating certainty.
