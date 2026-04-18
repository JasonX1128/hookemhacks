from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    output_path = Path(__file__).resolve().parents[1] / "fixtures" / "macro_headlines_demo.json"
    # TODO: Replace this with a deterministic ingest from a selected headline source or export.
    payload = [
        {
            "id": "headline-cpi-preview-1",
            "title": "Sticky services inflation commentary pushed rate-cut expectations lower",
            "timestamp": "2026-04-18T13:24:00Z",
            "source": "Demo Headlines Fixture",
            "snippet": "Macro desks highlighted stubborn services inflation and a less dovish path for cuts.",
            "importance": 0.77,
            "keywords": ["inflation", "cpi", "fed", "rates", "sticky"],
        }
    ]
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()

