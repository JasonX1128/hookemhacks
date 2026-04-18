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

  while (queue.length > 0) {
    const current = queue.shift();

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

function inferTickerFromScripts(): string | undefined {
  const scripts = Array.from(document.scripts)
    .map((script) => script.textContent ?? "")
    .filter(Boolean);

  const keyPatterns = [
    /"(?:ticker|market_ticker)"\s*:\s*"([^"]+)"/,
    /"(?:event_ticker)"\s*:\s*"([^"]+)"/,
  ];

  for (const script of scripts) {
    for (const pattern of keyPatterns) {
      const match = script.match(pattern);
      if (match?.[1]) {
        return match[1];
      }
    }
  }

  return undefined;
}

function inferTickerFromUrl(url: URL): string | undefined {
  const segments = url.pathname.split("/").map((segment) => segment.trim()).filter(Boolean);
  return [...segments].reverse().find((segment) => /^[A-Z0-9-]{6,}$/.test(segment));
}

function inferTickerFromDom(): string | undefined {
  const selectors = ["[data-market-id]", "[data-market-ticker]", "[data-ticker]"];

  for (const selector of selectors) {
    const element = document.querySelector(selector);
    if (!(element instanceof HTMLElement)) {
      continue;
    }

    const attributes = ["data-market-id", "data-market-ticker", "data-ticker"];
    for (const attribute of attributes) {
      const value = normalizeText(element.getAttribute(attribute));
      if (value) {
        return value;
      }
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

export function extractMarketMetadata(): MarketMetadata {
  const nextData = parseNextData();
  const url = new URL(window.location.href);
  const fallbackId = inferTickerFromUrl(url) || url.pathname.split("/").filter(Boolean).join(":") || url.hostname;
  const pageHeading = normalizeText(document.querySelector("h1")?.textContent);
  const metaTitle = extractMetaContent(["meta[property='og:title']", "meta[name='twitter:title']"]);
  const documentTitle = normalizeText(document.title.replace(/\s*\|\s*Kalshi\s*$/i, ""));
  const title = pageHeading || metaTitle || documentTitle || fallbackId;

  const marketId =
    deepFindString(nextData, ["ticker", "market_ticker", "event_ticker"]) ||
    inferTickerFromScripts() ||
    inferTickerFromDom() ||
    inferTickerFromUrl(url) ||
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
