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
MAX_FILTER_PROMPT_ARTICLES = 5
MAX_SYNTHESIS_PROMPT_ARTICLES = 3
MAX_PROMPT_TITLE_CHARS = 140
MAX_PROMPT_QUESTION_CHARS = 220
MAX_PROMPT_RULES_CHARS = 220
MAX_PROMPT_ARTICLE_TITLE_CHARS = 120
MAX_PROMPT_ARTICLE_SNIPPET_CHARS = 140
MAX_JSON_CALL_ATTEMPTS = 2
QUERY_PLAN_MAX_OUTPUT_TOKENS = 256
RELEVANCE_FILTER_MAX_OUTPUT_TOKENS = 128
SYNTHESIS_MAX_OUTPUT_TOKENS = 768
RETRY_MAX_OUTPUT_TOKENS = 1536
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
        "primary_query": {"type": "string"},
        "alt_queries": {"type": "array", "items": {"type": "string"}},
        "market_type": {"type": "string"},
    },
    "required": ["primary_query", "alt_queries", "market_type"],
}
RELEVANCE_FILTER_SCHEMA = {
    "type": "object",
    "properties": {
        "relevant_indices": {"type": "array", "items": {"type": "integer"}},
        "reasoning": {"type": "string"},
    },
    "required": ["relevant_indices"],
}
SYNTHESIS_SCHEMA = {
    "type": "object",
    "properties": {
        "analysis": {"type": "string"},
        "used_market_rules": {"type": "boolean"},
    },
    "required": ["analysis", "used_market_rules"],
}

MARKET_TYPE_KEYWORDS = {
    "sports": ["game", "match", "championship", "tournament", "team", "player", "score", "win", "vs", "league"],
    "politics": ["election", "vote", "president", "congress", "senate", "governor", "poll", "candidate", "party"],
    "economics": ["fed", "inflation", "cpi", "gdp", "jobs", "unemployment", "rate", "recession", "fomc", "treasury"],
    "crypto": ["bitcoin", "btc", "eth", "crypto", "token", "blockchain"],
    "weather": ["hurricane", "storm", "temperature", "weather", "climate"],
}


@dataclass(frozen=True)
class SearchQueryPlan:
    primary_query: str
    alt_queries: list[str]
    market_type: str
    must_include_terms: list[str]
    time_focus: str | None = None

    @property
    def all_queries(self) -> list[str]:
        return [self.primary_query] + self.alt_queries[:2]


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
        detected_type = self._detect_market_type(context)
        fallback = SearchQueryPlan(
            primary_query=self._fallback_query(context),
            alt_queries=self._generate_fallback_alt_queries(context, detected_type),
            market_type=detected_type,
            must_include_terms=self._must_include_terms(context),
            time_focus=context.clickedTimestamp,
        )
        if not self.query_model:
            logger.debug("[QueryPlan] Using fallback (no model): %s", fallback.primary_query[:60])
            return fallback

        prompt = self._build_query_plan_prompt(context)

        payload = self._call_model_json(
            model=self.query_model,
            prompt=prompt,
            schema=QUERY_PLAN_SCHEMA,
            temperature=0.15,
            max_output_tokens=QUERY_PLAN_MAX_OUTPUT_TOKENS,
            model_name=QUERY_MODEL,
        )
        if not isinstance(payload, dict):
            logger.debug("[QueryPlan] Model returned invalid payload, using fallback")
            return fallback

        primary = self._normalize_query(payload.get("primary_query")) or fallback.primary_query
        alt_queries = [
            self._normalize_query(q)
            for q in payload.get("alt_queries", [])
            if isinstance(q, str)
        ]
        alt_queries = [q for q in alt_queries if q and q != primary][:2]
        market_type = payload.get("market_type", detected_type) or detected_type

        result = SearchQueryPlan(
            primary_query=primary,
            alt_queries=alt_queries or fallback.alt_queries,
            market_type=market_type,
            must_include_terms=fallback.must_include_terms,
            time_focus=fallback.time_focus,
        )
        logger.debug(
            "[QueryPlan] Generated queries: primary='%s', alts=%s, type=%s",
            result.primary_query[:50],
            [q[:30] for q in result.alt_queries],
            result.market_type,
        )
        return result

    def _detect_market_type(self, context: MarketClickContext) -> str:
        text = f"{context.marketTitle} {context.marketQuestion} {context.marketSubtitle or ''}".lower()
        scores = {}
        for market_type, keywords in MARKET_TYPE_KEYWORDS.items():
            scores[market_type] = sum(1 for kw in keywords if kw in text)
        best_type = max(scores, key=lambda k: scores[k])
        return best_type if scores[best_type] > 0 else "other"

    def _generate_fallback_alt_queries(self, context: MarketClickContext, market_type: str) -> list[str]:
        base = context.marketTitle
        alt_queries = []
        if context.marketSubtitle:
            alt_queries.append(f"{context.marketSubtitle} news")
        if market_type == "sports":
            alt_queries.append(f"{base} score result")
        elif market_type == "economics":
            alt_queries.append(f"{base} announcement")
        elif market_type == "politics":
            alt_queries.append(f"{base} poll")
        else:
            alt_queries.append(f"{base} latest news")
        return alt_queries[:2]

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
            credibility = article.credibilityScore or 0.0
            temporal = article.temporalScore or 0.3

            combined = clamp_score(
                0.40 * similarity +
                0.25 * alignment +
                0.20 * temporal +
                0.15 * max(0, credibility + 0.5)
            )
            ranked_articles.append(
                NewsArticle(
                    title=article.title,
                    url=article.url,
                    source=article.source,
                    snippet=article.snippet,
                    date=article.date,
                    relevanceScore=combined,
                    alignmentScore=alignment,
                    credibilityScore=credibility,
                    temporalScore=temporal,
                )
            )

        ranked_articles.sort(
            key=lambda a: (a.relevanceScore or 0.0, a.temporalScore or 0.0, a.credibilityScore or 0.0),
            reverse=True,
        )

        logger.debug(
            "[Ranking] Top 3 articles: %s",
            [(a.title[:40], f"rel={a.relevanceScore:.2f}", f"temp={a.temporalScore:.2f}") for a in ranked_articles[:3]],
        )

        relevant_articles = [a for a in ranked_articles if (a.relevanceScore or 0.0) >= 0.20]
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
        articles: list[NewsArticle],
        move: MoveSummary | None = None,
    ) -> tuple[SynthesizedCatalyst | None, list[NewsArticle]]:
        if not self.synthesis_model:
            return None, []

        relevant_articles = self._filter_relevant_articles(context, move, articles)
        logger.debug(
            "[Synthesis] Filtered to %d relevant articles from %d candidates",
            len(relevant_articles),
            len(articles),
        )

        prompt = self._build_synthesis_prompt(context, relevant_articles)
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

        used_market_rules = bool(payload.get("used_market_rules"))

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

        logger.debug("[Synthesis] Generated analysis (confidence=%.2f): %s", confidence, analysis[:200])
        return catalyst, relevant_articles

    def _filter_relevant_articles(
        self,
        context: MarketClickContext,
        move: MoveSummary | None,
        articles: list[NewsArticle],
    ) -> list[NewsArticle]:
        if not articles:
            return []
        if len(articles) <= 3:
            return articles
        if not self.synthesis_model:
            return articles[:MAX_SYNTHESIS_ARTICLES]

        article_list = "\n".join(
            self._format_article_prompt_block(i, article, include_snippet=False)
            for i, article in enumerate(articles[:MAX_FILTER_PROMPT_ARTICLES])
        )

        prompt = self._build_relevance_filter_prompt(context, article_list)

        payload = self._call_model_json(
            model=self.synthesis_model,
            prompt=prompt,
            schema=RELEVANCE_FILTER_SCHEMA,
            temperature=0.0,
            max_output_tokens=RELEVANCE_FILTER_MAX_OUTPUT_TOKENS,
            model_name=f"{SYNTHESIS_MODEL}-filter",
        )

        if not isinstance(payload, dict):
            logger.debug("[Filter] Model returned invalid payload, using top articles")
            return articles[:MAX_SYNTHESIS_ARTICLES]

        indices = [
            i for i in payload.get("relevant_indices", [])
            if isinstance(i, int) and 0 <= i < len(articles)
        ]

        if not indices:
            logger.debug("[Filter] No relevant articles found, using top 3")
            return articles[:3]

        logger.debug("[Filter] Selected indices: %s", indices)
        return [articles[i] for i in indices[:MAX_SYNTHESIS_ARTICLES]]

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
        articles: list[NewsArticle],
    ) -> str:
        article_blocks = "\n".join(
            self._format_article_prompt_block(index, article, include_snippet=True)
            for index, article in enumerate(articles[:MAX_SYNTHESIS_PROMPT_ARTICLES])
        )

        market_block = self._format_market_prompt_block(context, include_rules=True)
        articles_block = article_blocks if article_blocks else "None"

        return (
            "Explain the most likely recent catalyst for this market in 1-2 sentences.\n"
            f"{market_block}\n"
            f"Articles:\n{articles_block}\n"
            "Rules:\n"
            "- Cite the concrete event, result, release, injury, trade, or announcement if present.\n"
            "- Use numbers or scores only if they appear in the articles.\n"
            '- If no concrete catalyst appears, return "Tracking <brief market subject>."\n'
            'Return JSON: {"analysis":"...","used_market_rules":true|false}'
        )

    def _build_query_plan_prompt(self, context: MarketClickContext) -> str:
        return (
            "Create short web search queries for recent events that could move this market.\n"
            f"{self._format_market_prompt_block(context, include_rules=False)}\n"
            "Return JSON with primary_query, alt_queries, market_type.\n"
            "Rules:\n"
            "- primary_query should be 3-8 words\n"
            "- alt_queries should contain at most 2 short alternatives\n"
            "- focus on recent events/results/releases, not odds or predictions\n"
            "- market_type must be one of sports, politics, economics, crypto, weather, entertainment, other"
        )

    def _build_relevance_filter_prompt(self, context: MarketClickContext, article_list: str) -> str:
        return (
            "Select article indices that contain concrete recent events relevant to this market.\n"
            f"Market: {self._truncate_prompt_text(context.marketTitle, MAX_PROMPT_TITLE_CHARS)}\n"
            f"Articles:\n{article_list}\n"
            "Keep articles about results, releases, injuries, trades, or announcements.\n"
            "Skip previews, odds, opinion, and generic explainers.\n"
            'Return JSON: {"relevant_indices":[...]}'
        )

    def _format_market_prompt_block(self, context: MarketClickContext, *, include_rules: bool) -> str:
        lines = [
            f"Market: {self._truncate_prompt_text(context.marketTitle, MAX_PROMPT_TITLE_CHARS)}",
        ]

        normalized_title = self._comparison_text(context.marketTitle)
        normalized_question = self._comparison_text(context.marketQuestion)
        if normalized_question and normalized_question != normalized_title:
            lines.append(
                f"Question: {self._truncate_prompt_text(context.marketQuestion, MAX_PROMPT_QUESTION_CHARS)}"
            )

        if include_rules and context.marketRulesPrimary:
            normalized_rules = self._comparison_text(context.marketRulesPrimary)
            if normalized_rules and normalized_rules not in {normalized_title, normalized_question}:
                lines.append(
                    f"Resolution: {self._truncate_prompt_text(context.marketRulesPrimary, MAX_PROMPT_RULES_CHARS)}"
                )

        return "\n".join(lines)

    def _format_article_prompt_block(
        self,
        index: int,
        article: NewsArticle,
        *,
        include_snippet: bool,
    ) -> str:
        header = (
            f"[{index}] "
            f"{self._truncate_prompt_text(article.title, MAX_PROMPT_ARTICLE_TITLE_CHARS)} | "
            f"{self._truncate_prompt_text(article.source, 40)} | "
            f"{self._truncate_prompt_text(article.date or 'Unknown', 32)}"
        )
        if not include_snippet:
            return header

        snippet = self._truncate_prompt_text(article.snippet or "No snippet available.", MAX_PROMPT_ARTICLE_SNIPPET_CHARS)
        return f"{header}\nSnippet: {snippet}"

    def _truncate_prompt_text(self, value: str | None, max_chars: int) -> str:
        normalized = " ".join((value or "").split()).strip()
        if len(normalized) <= max_chars:
            return normalized
        return f"{normalized[: max_chars - 3].rstrip()}..."

    def _comparison_text(self, value: str | None) -> str:
        return " ".join((value or "").split()).strip().lower()

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
            logger.debug("[Gemini] Skipping %s call (model not initialized)", model_name)
            return None

        import time
        for attempt in range(MAX_JSON_CALL_ATTEMPTS):
            attempt_temperature = temperature if attempt == 0 else 0.0
            attempt_max_tokens = (
                max_output_tokens
                if attempt == 0
                else min(RETRY_MAX_OUTPUT_TOKENS, max(max_output_tokens * 2, max_output_tokens + 256))
            )

            logger.debug(
                "[Gemini] %s attempt %d/%d (temp=%.1f, max_tokens=%d, prompt_chars=%d)",
                model_name,
                attempt + 1,
                MAX_JSON_CALL_ATTEMPTS,
                attempt_temperature,
                attempt_max_tokens,
                len(prompt),
            )
            call_start = time.perf_counter()

            response = self._generate_model_response(
                model=model,
                prompt=prompt,
                schema=schema,
                temperature=attempt_temperature,
                max_output_tokens=attempt_max_tokens,
                model_name=model_name,
            )

            call_duration = time.perf_counter() - call_start
            if response is None:
                logger.debug("[Gemini] %s returned None in %.2fs", model_name, call_duration)
                return None

            payload = self._parse_model_json_response(response, model_name=model_name)
            if payload is not None:
                logger.debug("[Gemini] %s succeeded in %.2fs", model_name, call_duration)
                return payload

            if attempt + 1 < MAX_JSON_CALL_ATTEMPTS:
                logger.warning(
                    "%s returned malformed JSON on attempt %d/%d; retrying once with more output headroom",
                    model_name,
                    attempt + 1,
                    MAX_JSON_CALL_ATTEMPTS,
                )

        logger.warning("[Gemini] %s failed after %d attempts", model_name, MAX_JSON_CALL_ATTEMPTS)
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
