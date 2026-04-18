from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    output_path = Path(__file__).resolve().parents[1] / "fixtures" / "macro_calendar.json"
    # TODO: Replace the inline payload with a repeatable ingest from a chosen macro calendar source.
    payload = [
        {
            "id": "event-fed-minutes",
            "title": "FOMC minutes release window",
            "timestamp": "2026-04-18T13:00:00Z",
            "source": "Macro Calendar Fixture",
            "snippet": "Scheduled event worth checking for inflation, rates, and macro proxy markets.",
            "importance": 0.78,
            "keywords": ["cpi", "inflation", "fed", "rates", "yield"],
        }
    ]
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()

