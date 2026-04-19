export interface MarketMetadata {
  marketId: string;
  marketTitle: string;
  marketQuestion: string;
}

function normalizeText(value: string | null | undefined): string | undefined {
  const normalized = value?.replace(/\s+/g, " ").trim();
  return normalized || undefined;
}

function deepFindString(input: unknown, preferredKeys: string[]): string | undefined {
  const queue: unknown[] = [input];
  for (let index = 0; index < queue.length; index += 1) {
    const current = queue[index];

    if (!current || typeof current !== "object") {
      continue;
    }

    if (Array.isArray(current)) {
      queue.push(...current);
      continue;
    }

    const record = current as Record<string, unknown>;
    for (const key of preferredKeys) {
      if (typeof record[key] === "string" && record[key].trim()) {
        return record[key] as string;
      }
    }

    queue.push(...Object.values(record));
  }

  return undefined;
}

export function deepFindPreferredString(input: unknown, preferredKeys: string[]): string | undefined {
  const queue: unknown[] = [input];
  const firstMatchByKey = new Map<string, string>();
  const keySet = new Set(preferredKeys);

  for (let index = 0; index < queue.length; index += 1) {
    const current = queue[index];

    if (!current || typeof current !== "object") {
      continue;
    }

    if (Array.isArray(current)) {
      queue.push(...current);
      continue;
    }

    const record = current as Record<string, unknown>;
    for (const [key, value] of Object.entries(record)) {
      if (!keySet.has(key) || firstMatchByKey.has(key)) {
        continue;
      }

      if (typeof value === "string" && value.trim()) {
        firstMatchByKey.set(key, value);
      }
    }

    if (firstMatchByKey.size === keySet.size) {
      break;
    }

    queue.push(...Object.values(record));
  }

  for (const key of preferredKeys) {
    const match = firstMatchByKey.get(key);
    if (match) {
      return match;
    }
  }

  return undefined;
}

function extractMetaContent(selectors: string[]): string | undefined {
  for (const selector of selectors) {
    const content = document.querySelector<HTMLMetaElement>(selector)?.content;
    const normalized = normalizeText(content);
    if (normalized) {
      return normalized;
    }
  }

  return undefined;
}

function parseNextData(): Record<string, unknown> | undefined {
  const script = document.getElementById("__NEXT_DATA__");
  if (!script?.textContent) {
    return undefined;
  }

  try {
    return JSON.parse(script.textContent) as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

export function inferTickerFromScriptTexts(scriptTexts: string[]): string | undefined {
  const keyPatterns = [
    /"market_ticker"\s*:\s*"([^"]+)"/,
    /"ticker"\s*:\s*"([^"]+)"/,
    /"(?:event_ticker)"\s*:\s*"([^"]+)"/,
  ];

  for (const pattern of keyPatterns) {
    for (const scriptText of scriptTexts) {
      const match = scriptText.match(pattern);
      if (match?.[1]) {
        return match[1];
      }
    }
  }

  return undefined;
}

function inferTickerFromScripts(): string | undefined {
  const scripts = Array.from(document.scripts)
    .map((script) => script.textContent ?? "")
    .filter(Boolean);

  return inferTickerFromScriptTexts(scripts);
}

function isLikelyKalshiMarketId(segment: string): boolean {
  return /^k[a-z0-9-]{5,}$/i.test(segment);
}

export function inferTickerFromUrl(url: URL): string | undefined {
  const segments = url.pathname.split("/").map((segment) => segment.trim()).filter(Boolean);
  return [...segments].reverse().find((segment) => isLikelyKalshiMarketId(segment));
}

function inferTickerFromDom(): string | undefined {
  if (typeof document === "undefined") {
    return undefined;
  }

  const attributes = ["data-market-ticker", "data-ticker", "data-market-id"];

  for (const attribute of attributes) {
    const element = document.querySelector(`[${attribute}]`);
    if (!(element instanceof HTMLElement)) {
      continue;
    }

    const value = normalizeText(element.getAttribute(attribute));
    if (value) {
      return value;
    }
  }

  return undefined;
}

function inferQuestionFromPage(title: string): string {
  const heading = normalizeText(document.querySelector("h1")?.textContent);
  if (heading?.includes("?")) {
    return heading;
  }

  const candidateParagraph = Array.from(document.querySelectorAll("p, h2, h3"))
    .map((element) => normalizeText(element.textContent) ?? "")
    .find((text) => text.includes("?"));

  return (
    candidateParagraph ||
    extractMetaContent(["meta[name='description']", "meta[property='og:description']"]) ||
    title
  );
}

export function resolvePreferredMarketId(nextData: unknown, scriptTexts: string[], url: URL): string | undefined {
  const marketTicker =
    deepFindPreferredString(nextData, ["market_ticker"]) ||
    inferTickerFromScriptTexts(
      scriptTexts.filter((scriptText) => /"market_ticker"\s*:/.test(scriptText)),
    );

  return (
    marketTicker ||
    inferTickerFromDom() ||
    inferTickerFromUrl(url) ||
    deepFindPreferredString(nextData, ["ticker", "event_ticker"]) ||
    inferTickerFromScriptTexts(scriptTexts.filter((scriptText) => /"(?:ticker|event_ticker)"\s*:/.test(scriptText)))
  );
}

export function extractMarketMetadata(): MarketMetadata {
  const nextData = parseNextData();
  const url = new URL(window.location.href);
  const fallbackId = inferTickerFromUrl(url) || url.pathname.split("/").filter(Boolean).join(":") || url.hostname;
  const pageHeading = normalizeText(document.querySelector("h1")?.textContent);
  const metaTitle = extractMetaContent(["meta[property='og:title']", "meta[name='twitter:title']"]);
  const documentTitle = normalizeText(document.title.replace(/\s*\|\s*Kalshi\s*$/i, ""));
  const title = pageHeading || metaTitle || documentTitle || fallbackId;

  const marketId =
    resolvePreferredMarketId(
      nextData,
      Array.from(document.scripts).map((script) => script.textContent ?? "").filter(Boolean),
      url,
    ) ||
    fallbackId;

  const marketTitle =
    deepFindString(nextData, ["title", "market_title", "subtitle", "name"]) ||
    title;

  const marketQuestion =
    deepFindString(nextData, ["rules_primary", "question", "subtitle", "description"]) ||
    inferQuestionFromPage(marketTitle);

  return {
    marketId,
    marketTitle,
    marketQuestion,
  };
}
