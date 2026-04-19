import { describe, expect, it } from "vitest";

import {
  deepFindPreferredString,
  inferTickerFromScriptTexts,
  inferTickerFromUrl,
  resolvePreferredMarketId,
} from "../src/content/metadataExtractor";

describe("metadata extractor helpers", () => {
  it("prefers a nested market_ticker over a shallower event-level ticker", () => {
    const nextData = {
      props: {
        pageProps: {
          ticker: "KXWTIMINM-26APR30",
          market: {
            market_ticker: "KXWTIMINM-26APR30-T74",
          },
        },
      },
    };

    expect(deepFindPreferredString(nextData, ["market_ticker", "ticker", "event_ticker"])).toBe(
      "KXWTIMINM-26APR30-T74",
    );
  });

  it("prefers market_ticker matches across all script blocks before generic ticker fields", () => {
    const scripts = [
      '{"event_ticker":"KXWTIMINM-26APR30","ticker":"KXWTIMINM-26APR30"}',
      '{"market_ticker":"KXWTIMINM-26APR30-T74"}',
    ];

    expect(inferTickerFromScriptTexts(scripts)).toBe("KXWTIMINM-26APR30-T74");
  });

  it("accepts lowercase Kalshi slugs from the page URL", () => {
    expect(
      inferTickerFromUrl(
        new URL(
          "https://kalshi.com/markets/kxbbchartpositionsong/what-position-will-songalbum-be-on-the-billboard-chart/kxbbchartpositionsong-26apr25swi",
        ),
      ),
    ).toBe("kxbbchartpositionsong-26apr25swi");
  });

  it("prefers the URL slug over generic event-level tickers when no market_ticker is present", () => {
    const nextData = {
      props: {
        pageProps: {
          ticker: "KXBBCHARTPOSITIONSONG",
          event_ticker: "KXBBCHARTPOSITIONSONG-26APR25SWI",
        },
      },
    };

    expect(
      resolvePreferredMarketId(
        nextData,
        ['{"ticker":"KXBBCHARTPOSITIONSONG","event_ticker":"KXBBCHARTPOSITIONSONG-26APR25SWI"}'],
        new URL(
          "https://kalshi.com/markets/kxbbchartpositionsong/what-position-will-songalbum-be-on-the-billboard-chart/kxbbchartpositionsong-26apr25swi",
        ),
      ),
    ).toBe("kxbbchartpositionsong-26apr25swi");
  });
});
