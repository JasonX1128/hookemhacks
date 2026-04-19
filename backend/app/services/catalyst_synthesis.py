from __future__ import annotations

import json
import logging
import math
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

try:
    import vertexai
    from vertexai.generative_models import GenerationConfig, GenerativeModel
    from vertexai.language_models import TextEmbeddingModel
except ImportError:  # pragma: no cover - exercised only when optional deps are absent
    vertexai = None
    GenerationConfig = None
    GenerativeModel = None
    TextEmbeddingModel = None

from backend.app.schemas.contracts import EvidenceSource, MarketClickContext, MoveSummary, SynthesizedCatalyst
from backend.app.services.news_search import NewsArticle
from backend.app.services.utils import clamp_score, tokenize_text

logger = logging.getLogger(__name__)

QUERY_MODEL = "gemini-2.5-flash"
SYNTHESIS_MODEL = "gemini-2.5-flash"
EMBEDDING_MODEL = "text-embedding-005"
REQUEST_TIMEOUT_SECONDS = 20
MAX_SYNTHESIS_ARTICLES = 5
MAX_RULES_CHARS = 2500
MAX_JSON_CALL_ATTEMPTS = 2
QUERY_PLAN_MAX_OUTPUT_TOKENS = 512
SYNTHESIS_MAX_OUTPUT_TOKENS = 1536
RETRY_MAX_OUTPUT_TOKENS = 3072
GENERIC_TITLE_TOKENS = {
    "will",
    "market",
    "markets",
    "question",
    "price",
    "move",
    "moves",
    "kalshi",
    "track",
    "prediction",
    "probability",
    "chance",
    "year",
}
QUERY_PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string"},
        "must_include_terms": {"type": "array", "items": {"type": "string"}},
        "time_focus": {"type": "string"},
    },
    "required": ["query"],
}
SYNTHESIS_SCHEMA = {
    "type": "object",
    "properties": {
        "analysis": {"type": "string"},
        "relevant_indices": {"type": "array", "items": {"type": "integer"}},
        "used_market_rules": {"type": "boolean"},
    },
    "required": ["analysis", "relevant_indices", "used_market_rules"],
}


@dataclass(frozen=True)
class SearchQueryPlan:
    query: str
    must_include_terms: list[str]
    time_focus: str | None = None


class CatalystSynthesisService:
    def __init__(self, project_id: str | None = None, location: str = "us-central1") -> None:
        self.project_id = project_id
        self.location = location
        self.query_model: Any | None = None
        self.synthesis_model: Any | None = None
        self.embedding_model: Any | None = None

        if project_id and vertexai is not None and GenerativeModel is not None:
            try:
                vertexai.init(project=project_id, location=location)
                self.query_model = GenerativeModel(QUERY_MODEL)
                self.synthesis_model = GenerativeModel(SYNTHESIS_MODEL)
                if TextEmbeddingModel is not None:
                    self.embedding_model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)
            except Exception as exc:
                logger.warning("Vertex AI initialization failed: %s", exc)
                self.query_model = None
                self.synthesis_model = None
                self.embedding_model = None

    def plan_search_query(self, context: MarketClickContext) -> SearchQueryPlan:
        fallback = SearchQueryPlan(
            query=self._fallback_query(context),
            must_include_terms=self._must_include_terms(context),
            time_focus=context.clickedTimestamp,
        )
        if not self.query_model:
            return fallback

        prompt = f"""You are improving a news search query for a Kalshi market move analysis.

Return a compact query that is likely to surface catalyst news for this exact market.
Prefer specific entities, event names, and official release names over generic prediction-market wording.

MARKET TICKER: {context.marketId}
MARKET TITLE: {context.marketTitle}
MARKET SUBTITLE: {context.marketSubtitle or "None"}
MARKET QUESTION: {context.marketQuestion}
MARKET RULES: {(context.marketRulesPrimary or "None")[:MAX_RULES_CHARS]}
CLICKED TIMESTAMP: {context.clickedTimestamp}
WINDOW START: {context.windowStart}
WINDOW END: {context.windowEnd}
"""

        payload = self._call_model_json(
            model=self.query_model,
            prompt=prompt,
            schema=QUERY_PLAN_SCHEMA,
            temperature=0.1,
            max_output_tokens=QUERY_PLAN_MAX_OUTPUT_TOKENS,
            model_name=QUERY_MODEL,
        )
        if not isinstance(payload, dict):
            return fallback

        query = self._normalize_query(payload.get("query")) or fallback.query
        must_include_terms = [
            self._normalize_query(term)
            for term in payload.get("must_include_terms", [])
            if isinstance(term, str)
        ]
        must_include_terms = [term for term in must_include_terms if term][:5]
        for term in must_include_terms[:2]:
            if term.lower() not in query.lower():
                query = f'{query} "{term}"'.strip()

        return SearchQueryPlan(
            query=query,
            must_include_terms=must_include_terms or fallback.must_include_terms,
            time_focus=self._normalize_query(payload.get("time_focus")) or fallback.time_focus,
        )

    def rank_articles(
        self,
        context: MarketClickContext,
        articles: list[NewsArticle],
        *,
        limit: int = MAX_SYNTHESIS_ARTICLES,
    ) -> list[NewsArticle]:
        if not articles:
            return []

        market_text = self._market_text(context)
        article_texts = [self._article_text(article) for article in articles]
        similarity_scores = self._similarity_scores(market_text, article_texts)

        ranked_articles: list[NewsArticle] = []
        for article, similarity in zip(articles, similarity_scores):
            alignment = self._article_alignment(context, article)
            combined = clamp_score(0.7 * similarity + 0.3 * alignment)
            ranked_articles.append(
                NewsArticle(
                    title=article.title,
                    url=article.url,
                    source=article.source,
                    snippet=article.snippet,
                    date=article.date,
                    relevanceScore=combined,
                    alignmentScore=alignment,
                )
            )

        ranked_articles.sort(
            key=lambda article: ((article.relevanceScore or 0.0), (article.alignmentScore or 0.0)),
            reverse=True,
        )
        relevant_articles = [article for article in ranked_articles if (article.relevanceScore or 0.0) >= 0.18]
        minimum_results = min(3, len(ranked_articles))
        if len(relevant_articles) < minimum_results:
            relevant_articles = ranked_articles

        return relevant_articles[:limit]

    def compute_article_alignment_confidence(
        self,
        context: MarketClickContext,
        articles: list[NewsArticle],
    ) -> float:
        if not articles:
            return 0.34 if context.marketRulesPrimary else 0.12

        alignments = [
            article.alignmentScore
            if article.alignmentScore is not None
            else self._article_alignment(context, article)
            for article in articles
        ]
        average_alignment = sum(alignments) / len(alignments)
        direct_mentions = sum(1 for score in alignments if score >= 0.34)
        mention_ratio = direct_mentions / len(alignments)
        return clamp_score(0.18 + 0.42 * average_alignment + 0.4 * mention_ratio)

    def synthesize(
        self,
        context: MarketClickContext,
        move: MoveSummary,
        articles: list[NewsArticle],
    ) -> tuple[SynthesizedCatalyst | None, list[NewsArticle]]:
        if not self.synthesis_model:
            return None, []

        prompt = self._build_synthesis_prompt(context, move, articles)
        payload = self._call_model_json(
            model=self.synthesis_model,
            prompt=prompt,
            schema=SYNTHESIS_SCHEMA,
            temperature=0.2,
            max_output_tokens=SYNTHESIS_MAX_OUTPUT_TOKENS,
            model_name=SYNTHESIS_MODEL,
        )
        if not isinstance(payload, dict):
            return None, []

        analysis = payload.get("analysis")
        if not isinstance(analysis, str) or not analysis.strip():
            return None, []

        relevant_indices = [
            index
            for index in payload.get("relevant_indices", [])
            if isinstance(index, int) and 0 <= index < len(articles)
        ]
        relevant_articles = [articles[index] for index in relevant_indices]
        used_market_rules = bool(payload.get("used_market_rules"))

        if not relevant_articles and articles and not used_market_rules:
            relevant_articles = articles[: min(3, len(articles))]

        confidence = self._synthesis_confidence(
            context=context,
            articles=relevant_articles,
            used_market_rules=used_market_rules,
        )
        catalyst = SynthesizedCatalyst(
            summary=analysis.strip(),
            confidence=confidence,
            synthesizedAt=datetime.now(timezone.utc).isoformat(),
        )

        return catalyst, relevant_articles

    def articles_to_evidence(self, articles: list[NewsArticle]) -> list[EvidenceSource]:
        return [
            EvidenceSource(
                title=article.title,
                url=article.url,
                source=article.source,
                snippet=article.snippet,
                publishedAt=article.date,
            )
            for article in articles
        ]

    def _build_synthesis_prompt(
        self,
        context: MarketClickContext,
        move: MoveSummary,
        articles: list[NewsArticle],
    ) -> str:
        direction_text = {
            "up": "increased",
            "down": "decreased",
            "flat": "stayed roughly flat",
        }.get(move.moveDirection, "moved")
        article_blocks = "\n".join(
            (
                f"[{index}] {article.title}\n"
                f"Source: {article.source}\n"
                f"Published: {article.date or 'Unknown'}\n"
                f"Snippet: {(article.snippet or 'No snippet available.')[:400]}"
            )
            for index, article in enumerate(articles[:MAX_SYNTHESIS_ARTICLES])
        )

        return f"""You are analyzing a single Kalshi market price move.

Stay tightly grounded in this market. Ignore unrelated asset classes or macro topics unless they are directly tied to the market.

MARKET TICKER: {context.marketId}
MARKET TITLE: {context.marketTitle}
MARKET SUBTITLE: {context.marketSubtitle or "None"}
MARKET QUESTION: {context.marketQuestion}
OFFICIAL MARKET RULES: {(context.marketRulesPrimary or "None")[:MAX_RULES_CHARS]}
MOVE DIRECTION: {move.moveDirection}
MOVE SUMMARY: Price {direction_text} by {abs(move.moveMagnitude):.1%}
CLICKED TIMESTAMP: {context.clickedTimestamp}
WINDOW START: {context.windowStart}
WINDOW END: {context.windowEnd}

RETRIEVED ARTICLES:
{article_blocks if article_blocks else "None"}

Write 2-3 sentences explaining the most likely cause of this specific market move.
If the articles are weak or missing, fall back to reasoning from the official market rules and move context instead of inventing unrelated news.
Do not mention article indices, search quality, or that you are an AI model.
"""

    def _call_model_json(
        self,
        *,
        model: Any,
        prompt: str,
        schema: dict[str, Any],
        temperature: float,
        max_output_tokens: int,
        model_name: str,
    ) -> dict[str, Any] | None:
        if model is None or GenerationConfig is None:
            return None

        for attempt in range(MAX_JSON_CALL_ATTEMPTS):
            attempt_temperature = temperature if attempt == 0 else 0.0
            attempt_max_tokens = (
                max_output_tokens
                if attempt == 0
                else min(RETRY_MAX_OUTPUT_TOKENS, max(max_output_tokens * 2, max_output_tokens + 256))
            )

            response = self._generate_model_response(
                model=model,
                prompt=prompt,
                schema=schema,
                temperature=attempt_temperature,
                max_output_tokens=attempt_max_tokens,
                model_name=model_name,
            )
            if response is None:
                return None

            payload = self._parse_model_json_response(response, model_name=model_name)
            if payload is not None:
                return payload

            if attempt + 1 < MAX_JSON_CALL_ATTEMPTS:
                logger.warning(
                    "%s returned malformed JSON on attempt %d/%d; retrying once with more output headroom",
                    model_name,
                    attempt + 1,
                    MAX_JSON_CALL_ATTEMPTS,
                )

        return None

    def _similarity_scores(self, market_text: str, article_texts: list[str]) -> list[float]:
        if not article_texts:
            return []

        if self.embedding_model is not None:
            try:
                embeddings = self.embedding_model.get_embeddings([market_text, *article_texts], output_dimensionality=256)
                market_vector = embeddings[0].values
                return [clamp_score(max(0.0, self._cosine_similarity(market_vector, embedding.values))) for embedding in embeddings[1:]]
            except Exception as exc:
                logger.warning("Falling back to lexical relevance after embedding failure: %s", exc)

        market_counter = self._token_counter(market_text)
        return [
            clamp_score(max(0.0, self._counter_cosine_similarity(market_counter, self._token_counter(article_text))))
            for article_text in article_texts
        ]

    def _market_text(self, context: MarketClickContext) -> str:
        return "\n".join(
            part
            for part in [
                context.marketTitle,
                context.marketSubtitle or "",
                context.marketQuestion,
                context.marketRulesPrimary or "",
            ]
            if part
        )

    def _article_text(self, article: NewsArticle) -> str:
        return " ".join(part for part in [article.title, article.snippet or ""] if part)

    def _must_include_terms(self, context: MarketClickContext) -> list[str]:
        preferred_terms = []
        if context.marketSubtitle:
            preferred_terms.append(context.marketSubtitle)

        title_tokens = [
            token
            for token in tokenize_text(context.marketTitle)
            if token not in GENERIC_TITLE_TOKENS
        ]
        preferred_terms.extend(title_tokens[:3])
        return preferred_terms[:4]

    def _fallback_query(self, context: MarketClickContext) -> str:
        query = context.marketTitle
        if context.marketSubtitle and context.marketSubtitle.lower() not in query.lower():
            query = f'{query} "{context.marketSubtitle}"'
        return self._normalize_query(query) or context.marketId

    def _normalize_query(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        normalized = " ".join(value.split()).strip()
        return normalized or None

    def _article_alignment(self, context: MarketClickContext, article: NewsArticle) -> float:
        entity_tokens = {
            token for token in tokenize_text(f"{context.marketTitle} {context.marketSubtitle or ''}")
            if token not in GENERIC_TITLE_TOKENS
        }
        if not entity_tokens:
            return 0.2

        article_tokens = tokenize_text(self._article_text(article))
        if not article_tokens:
            return 0.0

        overlap_ratio = len(entity_tokens & article_tokens) / len(entity_tokens)
        return clamp_score(0.15 + 0.85 * overlap_ratio)

    def _synthesis_confidence(
        self,
        *,
        context: MarketClickContext,
        articles: list[NewsArticle],
        used_market_rules: bool,
    ) -> float:
        if articles:
            alignment = self.compute_article_alignment_confidence(context, articles)
            relevance = sum((article.relevanceScore or 0.0) for article in articles) / len(articles)
            return clamp_score(0.18 + 0.42 * alignment + 0.4 * relevance)

        if used_market_rules and context.marketRulesPrimary:
            return 0.36

        return 0.2

    def _token_counter(self, value: str) -> Counter[str]:
        return Counter(
            token.lower()
            for token in re.findall(r"[A-Za-z0-9]+", value)
            if len(token) >= 3
        )

    def _counter_cosine_similarity(self, left: Counter[str], right: Counter[str]) -> float:
        if not left or not right:
            return 0.0

        shared = set(left) & set(right)
        numerator = sum(left[token] * right[token] for token in shared)
        left_norm = math.sqrt(sum(count * count for count in left.values()))
        right_norm = math.sqrt(sum(count * count for count in right.values()))
        if left_norm == 0 or right_norm == 0:
            return 0.0
        return numerator / (left_norm * right_norm)

    def _cosine_similarity(self, left: list[float], right: list[float]) -> float:
        if not left or not right or len(left) != len(right):
            return 0.0

        numerator = sum(left_value * right_value for left_value, right_value in zip(left, right))
        left_norm = math.sqrt(sum(value * value for value in left))
        right_norm = math.sqrt(sum(value * value for value in right))
        if left_norm == 0 or right_norm == 0:
            return 0.0
        return numerator / (left_norm * right_norm)

    def _strip_json_fences(self, value: str) -> str:
        text = value.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return text

    def _generate_model_response(
        self,
        *,
        model: Any,
        prompt: str,
        schema: dict[str, Any],
        temperature: float,
        max_output_tokens: int,
        model_name: str,
    ) -> Any | None:
        def _generate() -> Any:
            return model.generate_content(
                prompt,
                generation_config=GenerationConfig(
                    temperature=temperature,
                    max_output_tokens=max_output_tokens,
                    response_mime_type="application/json",
                    response_schema=schema,
                ),
            )

        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_generate)
                return future.result(timeout=REQUEST_TIMEOUT_SECONDS)
        except FuturesTimeoutError:
            logger.warning("%s timed out after %ds", model_name, REQUEST_TIMEOUT_SECONDS)
            return None
        except Exception as exc:
            logger.warning("%s failed: %s", model_name, exc)
            return None

    def _parse_model_json_response(
        self,
        response: Any,
        *,
        model_name: str,
    ) -> dict[str, Any] | None:
        raw_text = self._extract_response_text(response)
        if not raw_text:
            logger.warning("%s returned an empty response", model_name)
            return None

        cleaned_text = self._strip_json_fences(raw_text)
        payload = self._best_effort_json_parse(cleaned_text)
        if payload is not None:
            return payload

        finish_reason = self._response_finish_reason(response)
        finish_message = self._response_finish_message(response)
        snippet = cleaned_text[:200].replace("\n", "\\n")
        logger.warning(
            "%s returned invalid JSON (finish_reason=%s, finish_message=%s): %s; snippet=%r",
            model_name,
            finish_reason,
            finish_message or "n/a",
            self._json_error_summary(cleaned_text),
            snippet,
        )
        return None

    def _extract_response_text(self, response: Any) -> str | None:
        try:
            raw_text = getattr(response, "text", None)
        except Exception:
            raw_text = None

        if isinstance(raw_text, str) and raw_text.strip():
            return raw_text

        try:
            response_dict = response.to_dict()
        except Exception:
            return None

        text_parts: list[str] = []
        for candidate in response_dict.get("candidates", []):
            content = candidate.get("content", {})
            for part in content.get("parts", []):
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    text_parts.append(text)

        joined = "".join(text_parts).strip()
        return joined or None

    def _best_effort_json_parse(self, value: str) -> dict[str, Any] | None:
        candidate_payloads = [value]
        extracted_object = self._extract_outer_json_object(value)
        if extracted_object and extracted_object not in candidate_payloads:
            candidate_payloads.append(extracted_object)

        for candidate in list(candidate_payloads):
            repaired = self._maybe_close_json(candidate)
            if repaired and repaired not in candidate_payloads:
                candidate_payloads.append(repaired)

        for candidate in candidate_payloads:
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed

        return None

    def _extract_outer_json_object(self, value: str) -> str | None:
        start = value.find("{")
        if start == -1:
            return None

        depth = 0
        in_string = False
        escaped = False
        for index in range(start, len(value)):
            char = value[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return value[start : index + 1]

        return value[start:] if value[start:].strip().startswith("{") else None

    def _maybe_close_json(self, value: str) -> str | None:
        if '"' in value.rstrip() and self._has_unterminated_string(value):
            return None

        brace_balance = 0
        bracket_balance = 0
        in_string = False
        escaped = False

        for char in value:
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
            elif char == "{":
                brace_balance += 1
            elif char == "}":
                brace_balance -= 1
            elif char == "[":
                bracket_balance += 1
            elif char == "]":
                bracket_balance -= 1

        if brace_balance <= 0 and bracket_balance <= 0:
            return None

        return f"{value}{']' * max(0, bracket_balance)}{'}' * max(0, brace_balance)}"

    def _has_unterminated_string(self, value: str) -> bool:
        in_string = False
        escaped = False
        for char in value:
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    in_string = False
            elif char == '"':
                in_string = True
        return in_string

    def _json_error_summary(self, value: str) -> str:
        try:
            json.loads(value)
            return "ok"
        except json.JSONDecodeError as exc:
            return str(exc)

    def _response_finish_reason(self, response: Any) -> str:
        candidates = getattr(response, "candidates", None)
        if not candidates:
            return "unknown"

        finish_reason = getattr(candidates[0], "finish_reason", None)
        return getattr(finish_reason, "name", str(finish_reason))

    def _response_finish_message(self, response: Any) -> str | None:
        candidates = getattr(response, "candidates", None)
        if not candidates:
            return None

        finish_message = getattr(candidates[0], "finish_message", None)
        return finish_message if isinstance(finish_message, str) and finish_message else None
