# Task Ownership

This splits the MVP into work streams so contributors can move independently without stepping on each other.

## Contracts And Docs

- Keep the TS and Python contracts aligned.
- Treat [docs/CONTRACTS.md](/Users/jasonxie/Documents/hookemhacks/docs/CONTRACTS.md) as the user-facing contract summary.
- Update the root README when setup or demo steps change.

## Extension Shell And UI

- Own the MV3 manifest, background worker, panel rendering, and extension storage settings.
- Preserve the mock flow so UI work can continue even when the live chart extractor is imperfect.
- Keep the side panel compact and demo-friendly.

## Chart Extraction

- Own Kalshi page metadata extraction and chart click mapping.
- Improve timestamp/price extraction only when there is strong DOM evidence.
- Leave TODO markers for brittle selectors or uncertain page internals.

## Backend Retrieval And Ranking

- Own `/health`, `/attribute_move`, move characterization, fixture-backed retrieval, and ranking.
- Keep language conservative and avoid overclaiming causality.
- Add public/no-key retrieval adapters only when they do not destabilize the demo path.

## Related-Market Analysis

- Own curated macro-universe matching, relation scoring, and lagging/disconnect annotations.
- Keep the first pass macro-focused and narrow.
- Do not brute-force all-pairs cointegration.

## Demo Polish

- Keep fixtures plausible and presentation-ready.
- Maintain both the real-click path and the mock fallback path in [docs/DEMO_SCRIPT.md](/Users/jasonxie/Documents/hookemhacks/docs/DEMO_SCRIPT.md).
- Prefer predictable local behavior over broad but fragile live integrations.

