from __future__ import annotations

import logging
from datetime import datetime, timezone

from google import genai
from google.genai import types

from backend.app.schemas.contracts import (
    EvidenceSource,
    MarketClickContext,
    MoveSummary,
    SynthesizedCatalyst,
)
from backend.app.services.news_search import NewsArticle

logger = logging.getLogger(__name__)

PRIMARY_MODEL = "gemini-2.0-flash"
FALLBACK_MODEL = "gemma-3-27b-it"
DEFAULT_TIMEOUT = 15.0


class CatalystSynthesisService:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key
        self.client: genai.Client | None = None
        if api_key:
            self.client = genai.Client(api_key=api_key)

    def _build_prompt(
        self,
        context: MarketClickContext,
        move: MoveSummary,
        articles: list[NewsArticle],
    ) -> str:
        direction_text = {
            "up": "increased",
            "down": "decreased",
            "flat": "remained stable",
        }.get(move.moveDirection, "moved")

        articles_text = "\n".join(
            f"- [{a.source}] {a.title}"
            + (f": {a.snippet[:200]}..." if a.snippet and len(a.snippet) > 200 else f": {a.snippet}" if a.snippet else "")
            for a in articles[:8]
        )

        return f"""You are a financial analyst explaining prediction market movements.

Market: {context.marketTitle}
Question: {context.marketQuestion}
Price Movement: {direction_text} by {abs(move.moveMagnitude):.1%}

Recent news articles:
{articles_text}

Based on the news above, write a concise 2-3 sentence explanation of the most likely catalyst for this price movement. Focus on the specific news event that most directly explains the market move. Be specific about what happened and why it affected this prediction market.

Respond with ONLY the explanation, no preamble or formatting."""

    def _call_model(self, model: str, prompt: str) -> str | None:
        if not self.client:
            return None

        try:
            response = self.client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.3,
                    max_output_tokens=256,
                ),
            )

            if response.text:
                return response.text.strip()

            logger.warning("%s returned empty response", model)
            return None

        except Exception as exc:
            logger.warning("%s failed: %s", model, exc)
            return None

    def synthesize(
        self,
        context: MarketClickContext,
        move: MoveSummary,
        articles: list[NewsArticle],
    ) -> SynthesizedCatalyst | None:
        if not self.client or not articles:
            return None

        prompt = self._build_prompt(context, move, articles)

        # Try primary model, fall back to Gemma
        summary = self._call_model(PRIMARY_MODEL, prompt)
        if not summary:
            logger.info("Falling back to %s", FALLBACK_MODEL)
            summary = self._call_model(FALLBACK_MODEL, prompt)

        if not summary:
            return None

        return SynthesizedCatalyst(
            summary=summary,
            confidence=0.7,
            synthesizedAt=datetime.now(timezone.utc).isoformat(),
        )

    def articles_to_evidence(self, articles: list[NewsArticle]) -> list[EvidenceSource]:
        return [
            EvidenceSource(
                title=a.title,
                url=a.url,
                source=a.source,
                snippet=a.snippet,
                publishedAt=a.date,
            )
            for a in articles
        ]
