from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    output_path = Path(__file__).resolve().parents[1] / "fixtures" / "macro_market_universe.json"
    # TODO: Expand the curated macro universe only after we validate relation quality on the initial economics + proxies set.
    payload = [
        {
            "marketId": "KXRATES-FEDCUT-SEP2026",
            "title": "Will the Fed cut by September 2026?",
            "question": "Will the Federal Reserve cut rates by September 2026?",
            "categoryScore": 0.88,
            "semanticBoost": 0.34,
            "historicalComovement": 0.79,
            "expectedReactionScore": 0.76,
            "residualZscore": 0.4,
            "proxyType": "rates_proxy",
            "note": "Rates-sensitive market that often reacts alongside inflation repricing."
        }
    ]
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
