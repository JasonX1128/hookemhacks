import type {
  AttributionResponse,
  CatalystCandidate,
  EvidenceSource,
  MoveDirection,
  RelatedMarket,
  RelatedMarketStatus,
  SynthesizedCatalyst,
} from "../shared/contracts";

const MAX_EVIDENCE_ITEMS = 3;
const MAX_RELATED_MARKETS = 3;
const MAX_WORTH_CHECKING_ITEMS = 2;
const MAX_SYNTHESIZED_EVIDENCE = 5;

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function humanize(value: string): string {
  return value
    .split("_")
    .filter(Boolean)
    .map((segment) => segment[0]?.toUpperCase() + segment.slice(1))
    .join(" ");
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatScore(score: number | undefined): string {
  if (score === undefined || Number.isNaN(score)) {
    return "n/a";
  }

  return `${Math.round(score * 100)}%`;
}

function formatPrice(price: number | undefined): string | undefined {
  if (price === undefined || Number.isNaN(price)) {
    return undefined;
  }

  return `${Math.round(price * 100)}c`;
}

function formatMoveMagnitude(direction: MoveDirection, magnitude: number): string {
  const points = Math.round(magnitude * 100);
  if (direction === "flat") {
    return `${points} pts flat`;
  }

  const prefix = direction === "down" ? "-" : "+";
  return `${prefix}${points} pts`;
}

function formatDirection(direction: MoveDirection): string {
  switch (direction) {
    case "up":
      return "Up move";
    case "down":
      return "Down move";
    default:
      return "Flat";
  }
}

function formatCandidateType(candidateType: CatalystCandidate["type"]): string {
  return humanize(candidateType);
}

function formatRelatedMarketStatus(status: RelatedMarketStatus | undefined): string {
  switch (status) {
    case "possibly_lagging":
      return "Possibly lagging";
    case "divergent":
      return "Divergent";
    default:
      return "Normal";
  }
}

function getRelatedMarketTone(status: RelatedMarketStatus | undefined): string {
  switch (status) {
    case "possibly_lagging":
      return " mme-pill-warn";
    case "divergent":
      return " mme-pill-alert";
    default:
      return "";
  }
}

function formatPriceTransition(response: AttributionResponse): string | undefined {
  const before = formatPrice(response.primaryMarket.priceBefore);
  const after = formatPrice(response.primaryMarket.priceAfter ?? response.primaryMarket.clickedPrice);

  if (before && after) {
    return `${before} -> ${after}`;
  }

  return after;
}

function dedupeCandidates(candidates: CatalystCandidate[]): CatalystCandidate[] {
  const seen = new Set<string>();
  return candidates.filter((candidate) => {
    if (seen.has(candidate.id)) {
      return false;
    }

    seen.add(candidate.id);
    return true;
  });
}

function isWorthCheckingMarket(market: RelatedMarket): boolean {
  if (market.status === "possibly_lagging" || market.status === "divergent") {
    return true;
  }

  if (typeof market.residualZscore === "number" && Math.abs(market.residualZscore) >= 1.5) {
    return true;
  }

  return market.note?.toLowerCase().includes("worth checking") ?? false;
}

function renderEmptyCopy(copy: string): string {
  return `<p class="mme-empty-copy">${escapeHtml(copy)}</p>`;
}

function renderLink(url: string | undefined): string {
  if (!url) {
    return "";
  }

  return `<a class="mme-link" href="${escapeHtml(url)}" target="_blank" rel="noreferrer">Open source</a>`;
}

function renderEvidenceItem(candidate: CatalystCandidate): string {
  const meta = `${escapeHtml(candidate.source)} • ${escapeHtml(formatTimestamp(candidate.timestamp))}`;
  const snippet = candidate.snippet
    ? `<p class="mme-list-copy">${escapeHtml(candidate.snippet)}</p>`
    : '<p class="mme-list-copy">No short summary was provided for this item.</p>';

  return `
    <article class="mme-list-row">
      <div class="mme-list-head">
        <strong class="mme-list-title">${escapeHtml(candidate.title)}</strong>
        <span class="mme-pill">${escapeHtml(formatCandidateType(candidate.type))}</span>
      </div>
      <span class="mme-list-meta">${meta}</span>
      ${snippet}
      <div class="mme-row-actions">
        <span class="mme-tag">Score ${formatScore(candidate.totalScore)}</span>
        ${renderLink(candidate.url)}
      </div>
    </article>
  `;
}

function renderRelatedMarket(market: RelatedMarket, highlight: boolean = false): string {
  const relationSummary = market.relationTypes.length
    ? market.relationTypes.slice(0, 2).map(humanize).join(" / ")
    : "Cross-market signal";
  const metaParts = [
    escapeHtml(market.marketId),
    `Relation ${formatScore(market.relationStrength)}`,
    typeof market.expectedReactionScore === "number"
      ? `Expected ${formatScore(market.expectedReactionScore)}`
      : undefined,
    typeof market.residualZscore === "number" ? `Residual ${market.residualZscore.toFixed(1)}z` : undefined,
  ].filter(Boolean);

  const copy = market.note ?? relationSummary;
  const highlightClass = highlight ? " mme-list-row-highlight" : "";

  return `
    <article class="mme-list-row${highlightClass}">
      <div class="mme-list-head">
        <strong class="mme-list-title">${escapeHtml(market.title)}</strong>
        <span class="mme-pill${getRelatedMarketTone(market.status)}">${escapeHtml(
          formatRelatedMarketStatus(market.status),
        )}</span>
      </div>
      <span class="mme-list-meta">${metaParts.join(" • ")}</span>
      <p class="mme-list-copy">${escapeHtml(copy)}</p>
    </article>
  `;
}

function renderMoveSummary(response: AttributionResponse): string {
  const clickedAt = formatTimestamp(response.primaryMarket.clickedTimestamp);
  const priceTransition = formatPriceTransition(response);
  const transitionCopy = priceTransition ? ` • ${escapeHtml(priceTransition)}` : "";

  return `
    <section class="mme-card mme-card-hero">
      <div class="mme-section-header">
        <div>
          <span class="mme-eyebrow">Move summary</span>
          <h2 class="mme-market-title">${escapeHtml(response.primaryMarket.marketTitle)}</h2>
        </div>
        <span class="mme-tag">${escapeHtml(clickedAt)}</span>
      </div>
      <p class="mme-market-question">${escapeHtml(response.primaryMarket.marketQuestion)}</p>
      <div class="mme-stat-grid">
        <div class="mme-stat">
          <span class="mme-stat-label">Move</span>
          <strong class="mme-stat-value">${formatMoveMagnitude(
            response.moveSummary.moveDirection,
            response.moveSummary.moveMagnitude,
          )}</strong>
        </div>
        <div class="mme-stat">
          <span class="mme-stat-label">Direction</span>
          <strong class="mme-stat-value">${escapeHtml(formatDirection(response.moveSummary.moveDirection))}</strong>
        </div>
        <div class="mme-stat">
          <span class="mme-stat-label">Jump score</span>
          <strong class="mme-stat-value">${formatScore(response.moveSummary.jumpScore)}</strong>
        </div>
        <div class="mme-stat">
          <span class="mme-stat-label">Confidence</span>
          <strong class="mme-stat-value">${formatScore(response.confidence)}</strong>
        </div>
      </div>
      <p class="mme-card-note">Clicked ${escapeHtml(clickedAt)}${transitionCopy}</p>
    </section>
  `;
}

function renderLikelyCatalyst(response: AttributionResponse): string {
  if (!response.topCatalyst) {
    return `
      <section class="mme-card">
        <div class="mme-section-header">
          <h3 class="mme-section-title">Likely catalyst</h3>
        </div>
        ${renderEmptyCopy("No clear catalyst surfaced for this move yet.")}
      </section>
    `;
  }

  return `
    <section class="mme-card">
      <div class="mme-section-header">
        <h3 class="mme-section-title">Likely catalyst</h3>
        <span class="mme-tag">Best match ${formatScore(response.topCatalyst.totalScore)}</span>
      </div>
      ${renderEvidenceItem(response.topCatalyst)}
    </section>
  `;
}

function renderSynthesizedCatalyst(catalyst: SynthesizedCatalyst | undefined): string {
  if (!catalyst) {
    return "";
  }

  const synthesizedAt = formatTimestamp(catalyst.synthesizedAt);

  return `
    <section class="mme-card mme-card-synthesized">
      <div class="mme-section-header">
        <h3 class="mme-section-title">AI Analysis</h3>
        <span class="mme-pill mme-pill-ai">Synthesized</span>
      </div>
      <p class="mme-synthesized-summary">${escapeHtml(catalyst.summary)}</p>
      <div class="mme-row-actions">
        <span class="mme-tag">Confidence ${formatScore(catalyst.confidence)}</span>
        <span class="mme-list-meta">${escapeHtml(synthesizedAt)}</span>
      </div>
    </section>
  `;
}

function renderSynthesizedEvidenceItem(source: EvidenceSource): string {
  const metaParts = [source.source];
  if (source.publishedAt) {
    metaParts.push(source.publishedAt);
  }

  return `
    <article class="mme-list-row">
      <div class="mme-list-head">
        <strong class="mme-list-title">${escapeHtml(source.title)}</strong>
      </div>
      <span class="mme-list-meta">${escapeHtml(metaParts.join(" • "))}</span>
      ${source.snippet ? `<p class="mme-list-copy">${escapeHtml(source.snippet)}</p>` : ""}
      <div class="mme-row-actions">
        <a class="mme-link" href="${escapeHtml(source.url)}" target="_blank" rel="noreferrer">Read article</a>
      </div>
    </article>
  `;
}

function renderSynthesizedEvidence(sources: EvidenceSource[] | undefined): string {
  if (!sources || sources.length === 0) {
    return "";
  }

  const items = sources.slice(0, MAX_SYNTHESIZED_EVIDENCE);

  return `
    <section class="mme-card">
      <div class="mme-section-header">
        <h3 class="mme-section-title">News Sources</h3>
        <span class="mme-tag">${items.length} article${items.length !== 1 ? "s" : ""}</span>
      </div>
      ${items.map(renderSynthesizedEvidenceItem).join("")}
    </section>
  `;
}

function renderEvidence(response: AttributionResponse): string {
  const evidenceItems = dedupeCandidates(
    response.evidence.filter((candidate) => candidate.id !== response.topCatalyst?.id),
  ).slice(0, MAX_EVIDENCE_ITEMS);

  return `
    <section class="mme-card">
      <div class="mme-section-header">
        <h3 class="mme-section-title">Evidence</h3>
        <span class="mme-tag">${evidenceItems.length || response.evidence.length ? "Nearby signals" : "Sparse"}</span>
      </div>
      ${
        evidenceItems.length
          ? evidenceItems.map(renderEvidenceItem).join("")
          : renderEmptyCopy("No extra evidence items were returned with this response.")
      }
    </section>
  `;
}

function renderRelatedMarkets(response: AttributionResponse): string {
  const markets = response.relatedMarkets.slice(0, MAX_RELATED_MARKETS);

  return `
    <section class="mme-card">
      <div class="mme-section-header">
        <h3 class="mme-section-title">Related markets</h3>
        <span class="mme-tag">${markets.length ? `${markets.length} surfaced` : "None yet"}</span>
      </div>
      ${
        markets.length
          ? markets.map((market) => renderRelatedMarket(market)).join("")
          : renderEmptyCopy("No related markets were returned for this move.")
      }
    </section>
  `;
}

function renderWorthChecking(response: AttributionResponse): string {
  const worthCheckingMarkets = response.relatedMarkets.filter(isWorthCheckingMarket).slice(0, MAX_WORTH_CHECKING_ITEMS);
  const alternativeCatalysts = dedupeCandidates(
    response.alternativeCatalysts.filter((candidate) => candidate.id !== response.topCatalyst?.id),
  ).slice(0, MAX_WORTH_CHECKING_ITEMS);

  if (worthCheckingMarkets.length) {
    return `
      <section class="mme-card mme-card-accent">
        <div class="mme-section-header">
          <h3 class="mme-section-title">Worth checking</h3>
          <span class="mme-tag">Potential disconnect</span>
        </div>
        <p class="mme-card-note">These look lagging, divergent, or unusual enough to double-check during the demo.</p>
        ${worthCheckingMarkets.map((market) => renderRelatedMarket(market, true)).join("")}
      </section>
    `;
  }

  if (alternativeCatalysts.length) {
    return `
      <section class="mme-card mme-card-accent">
        <div class="mme-section-header">
          <h3 class="mme-section-title">Worth checking</h3>
          <span class="mme-tag">Other nearby drivers</span>
        </div>
        <p class="mme-card-note">No obvious lagging market surfaced, so these secondary catalysts are the next fastest things to sanity-check.</p>
        ${alternativeCatalysts.map(renderEvidenceItem).join("")}
      </section>
    `;
  }

  return `
    <section class="mme-card mme-card-accent">
      <div class="mme-section-header">
        <h3 class="mme-section-title">Worth checking</h3>
      </div>
      ${renderEmptyCopy("Nothing stands out as lagging or divergent yet.")}
    </section>
  `;
}

export function renderAttributionResponse(response: AttributionResponse): string {
  const hasAiAnalysis = response.synthesizedCatalyst?.summary;

  return `
    <div class="mme-result-stack">
      ${renderMoveSummary(response)}
      ${renderSynthesizedCatalyst(response.synthesizedCatalyst)}
      ${renderSynthesizedEvidence(response.synthesizedEvidence)}
      ${hasAiAnalysis ? "" : renderLikelyCatalyst(response)}
      ${hasAiAnalysis ? "" : renderEvidence(response)}
      ${renderRelatedMarkets(response)}
      ${renderWorthChecking(response)}
    </div>
  `;
}
