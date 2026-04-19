from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


@dataclass(slots=True)
class MarketMetadataRecord:
    market_id: str
    ticker: str
    title: str
    question: str
    category: str | None = None
    families: list[str] = field(default_factory=list)
    open_time: str | None = None
    close_time: str | None = None
    resolution_time: str | None = None
    status: str | None = None
    tags: list[str] = field(default_factory=list)
    source: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["families"] = _dedupe_preserve_order(self.families)
        payload["tags"] = _dedupe_preserve_order(self.tags)
        return payload

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "MarketMetadataRecord":
        extras = payload.get("extra", {})
        normalized_families = payload.get("families") or []
        normalized_tags = payload.get("tags") or []
        if isinstance(normalized_families, str):
            normalized_families = [item.strip() for item in normalized_families.split("|") if item.strip()]
        if isinstance(normalized_tags, str):
            normalized_tags = [item.strip() for item in normalized_tags.split("|") if item.strip()]
        return cls(
            market_id=str(payload["market_id"]),
            ticker=str(payload.get("ticker") or payload["market_id"]),
            title=str(payload.get("title") or payload.get("question") or payload["market_id"]),
            question=str(payload.get("question") or payload.get("title") or payload["market_id"]),
            category=payload.get("category"),
            families=list(normalized_families),
            open_time=payload.get("open_time"),
            close_time=payload.get("close_time"),
            resolution_time=payload.get("resolution_time"),
            status=payload.get("status"),
            tags=list(normalized_tags),
            source=payload.get("source"),
            extra=dict(extras),
        )

    @property
    def combined_text(self) -> str:
        parts = [self.title, self.question, self.category or "", " ".join(self.families), " ".join(self.tags)]
        return " ".join(part for part in parts if part).strip()
