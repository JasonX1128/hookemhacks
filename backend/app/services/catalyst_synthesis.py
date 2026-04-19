from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone

import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

from backend.app.schemas.contracts import (
    EvidenceSource,
    MarketClickContext,
    MoveSummary,
    SynthesizedCatalyst,
)
from backend.app.services.news_search import NewsArticle

logger = logging.getLogger(__name__)

PRIMARY_MODEL = "gemini-2.5-flash"
REQUEST_TIMEOUT_SECONDS = 20


class CatalystSynthesisService:
    def __init__(self, project_id: str | None = None, location: str = "us-central1") -> None:
        self.project_id = project_id
        self.location = location
        self.model: GenerativeModel | None = None
        if project_id:
            vertexai.init(project=project_id, location=location)
            self.model = GenerativeModel(PRIMARY_MODEL)

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
            f"[{i}] {a.title} ({a.source}): {a.snippet[:300] if a.snippet else 'No snippet'}"
            for i, a in enumerate(articles[:10])
        )

        return f"""You are a prediction market analyst. Explain this specific price movement.

MARKET TITLE: {context.marketTitle}
MARKET QUESTION: {context.marketQuestion}
PRICE MOVEMENT: {direction_text} by {abs(move.moveMagnitude):.1%}

REFERENCE ARTICLES:
{articles_text if articles else "None"}

INSTRUCTIONS:
1. Your analysis MUST be about "{context.marketTitle}" - do not discuss any other topic
2. Write 2-3 sentences explaining what likely caused this specific market's price to move
3. Use relevant articles if available, otherwise use your knowledge about this specific market/event
4. Never mention the articles themselves in your response

Return only valid JSON: {{"relevant_indices": [0, 2], "analysis": "Your 2-3 sentence explanation about {context.marketTitle}."}}"""

    def _call_model(self, prompt: str) -> str | None:
        if not self.model:
            return None

        def _generate():
            return self.model.generate_content(
                prompt,
                generation_config=GenerationConfig(
                    temperature=0.3,
                    max_output_tokens=4096,
                ),
            )

        try:
            print("[DEBUG] Calling Gemini model...")
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_generate)
                response = future.result(timeout=REQUEST_TIMEOUT_SECONDS)

            print(f"[DEBUG] Gemini response received: {bool(response.text)}")
            print(f"[DEBUG] Finish reason: {response.candidates[0].finish_reason if response.candidates else 'N/A'}")
            if response.text:
                print(f"[DEBUG] Response length: {len(response.text)} chars")
                print(f"[DEBUG] Full response: {response.text}")
                return response.text.strip()

            logger.warning("%s returned empty response", PRIMARY_MODEL)
            return None

        except FuturesTimeoutError:
            print(f"[DEBUG] Gemini timed out after {REQUEST_TIMEOUT_SECONDS}s")
            logger.warning("%s timed out after %ds", PRIMARY_MODEL, REQUEST_TIMEOUT_SECONDS)
            return None
        except Exception as exc:
            print(f"[DEBUG] Gemini error: {exc}")
            logger.warning("%s failed: %s", PRIMARY_MODEL, exc)
            return None

    def _parse_response(self, response_text: str) -> tuple[str, list[int]]:
        text = response_text.strip()
        # Strip markdown code blocks
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)

        # Try to parse as direct JSON first
        try:
            data = json.loads(text)
            analysis = data.get("analysis", "")
            indices = data.get("relevant_indices", [])
            if isinstance(analysis, str) and analysis.strip():
                valid_indices = [i for i in indices if isinstance(i, int)]
                return analysis.strip(), valid_indices
        except json.JSONDecodeError:
            pass

        # Fall back to regex extraction
        try:
            # Find JSON object with greedy match
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                analysis = data.get("analysis", "")
                indices = data.get("relevant_indices", [])
                if isinstance(analysis, str) and analysis.strip():
                    valid_indices = [i for i in indices if isinstance(i, int)]
                    return analysis.strip(), valid_indices
        except (json.JSONDecodeError, AttributeError) as e:
            logger.warning("JSON parse failed: %s. Response: %s", e, text[:200])

        # If all else fails, return the raw text if it doesn't look like JSON
        if text and not text.startswith("{"):
            return text, []

        return "", []

    def synthesize(
        self,
        context: MarketClickContext,
        move: MoveSummary,
        articles: list[NewsArticle],
    ) -> tuple[SynthesizedCatalyst | None, list[NewsArticle]]:
        if not self.model:
            return None, []

        prompt = self._build_prompt(context, move, articles)
        print(f"[DEBUG] Market title: {context.marketTitle}")
        print(f"[DEBUG] Market question: {context.marketQuestion}")

        response_text = self._call_model(prompt)
        if not response_text:
            print("[DEBUG] No response text from model")
            return None, []

        analysis, relevant_indices = self._parse_response(response_text)
        print(f"[DEBUG] Parsed analysis: {analysis[:100] if analysis else 'None'}...")

        if not analysis:
            print("[DEBUG] Analysis is empty after parsing")
            return None, []

        relevant_articles = [
            articles[i] for i in relevant_indices
            if 0 <= i < len(articles)
        ]

        catalyst = SynthesizedCatalyst(
            summary=analysis,
            confidence=0.75 if relevant_articles else 0.5,
            synthesizedAt=datetime.now(timezone.utc).isoformat(),
        )

        return catalyst, relevant_articles

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
