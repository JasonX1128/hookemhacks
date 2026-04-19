  Current flow: Kalshi page → extract metadata → Serper news search → Gemini synthesis → UI

  Effectiveness (as observed)

  Weaknesses:
  1. Metadata extraction is brittle - relies on scraping __NEXT_DATA__ with fallbacks. We've seen title/question
  mismatches, wrong sport detection, and questions pulled from unrelated markets.
  2. Search query quality varies wildly - keyword-based league detection misses many cases. "Chiba Jets Funabashi"
  (Japanese B-League basketball) had no league detected. Politics markets often search with poor keywords.
  3. AI hallucination - Gemini sometimes ignores the prompt and invents Bitcoin/unrelated content, especially when
  articles don't match the market well.
  4. No price/time context in search - articles aren't filtered by the price movement window, so old unrelated news
  often dominates.
  5. No relevance scoring - all 10 Serper articles go to the AI regardless of actual relevance to the market.

  High-Impact Improvements

  1. Use Kalshi's API directly instead of DOM scraping - get ticker, title, rules_primary, subtitle authoritatively. No
  more mismatches.
  2. Time-windowed search - pass tbs=qdr:d (last 24h) or specific date ranges to Serper based on the clicked timestamp.
  Most catalysts are recent.
  3. Two-stage AI pipeline:
    - Stage 1 (cheap/fast): Have AI generate the optimal search query from market context
    - Stage 2: Synthesize with retrieved articles
  4. Pre-filter articles by relevance - embed market question + each article title, cosine similarity, keep top 3-5.
  Reduces noise.
  5. Structured output - use Gemini's response_mime_type: application/json with a schema instead of parsing JSON from
  text. Eliminates truncation/parsing issues.
  6. Fallback to market rules - Kalshi markets have official rules_primary text. If no good articles, the AI could
  reason from the rules + price history alone.
  7. Confidence from article alignment - measure how many articles directly mention entities in the market title; low
  overlap → low confidence shown to user.

  Want me to implement any of these? The biggest wins would be structured output (fixes truncation permanently) and
  Kalshi API for metadata (fixes wrong market issues at the root).