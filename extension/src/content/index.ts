import type { AttributionResponse, MarketClickContext } from "../shared/contracts";
import { buildMockAttributionResponse } from "../shared/fixtures/mockAttributionResponse";
import {
  ATTRIBUTE_MOVE_REQUEST,
  PANEL_BOOTSTRAP_REQUEST,
  UPDATE_SETTINGS_REQUEST,
  type AttributeMoveResponseMessage,
  type ErrorResponseMessage,
  type ExtensionSettings,
  type PanelBootstrapResponseMessage,
  type RequestMode,
  type RuntimeRequestMessage,
  type RuntimeResponseMessage,
  type UpdateSettingsResponseMessage,
} from "../shared/messages";
import { renderAttributionResponse } from "../ui/renderers";
import { initializeChartCapture } from "./chartCapture";
import { extractMarketMetadata, resolveMarketMetadata } from "./metadataExtractor";

const PANEL_HOST_ID = "market-move-explainer-root";
const URL_POLL_INTERVAL_MS = 1_500;
type ResultSource = "mock" | "live" | "fallback";

interface PanelState {
  isOpen: boolean;
  isLoading: boolean;
  errorMessage: string | null;
  noticeMessage: string | null;
  endpointUrl: string;
  currentContext: MarketClickContext;
  result: AttributionResponse | null;
  activeMode: RequestMode;
  resultSource: ResultSource;
}

const state: PanelState = {
  isOpen: true,
  isLoading: true,
  errorMessage: null,
  noticeMessage: null,
  endpointUrl: "http://127.0.0.1:8000/attribute_move",
  currentContext: buildFallbackContext(),
  result: null,
  activeMode: "mock",
  resultSource: "mock",
};

let panelApp: HTMLDivElement | null = null;
let currentUrl = globalThis.location.href;

function addMinutes(date: Date, minutes: number): string {
  return new Date(date.getTime() + minutes * 60_000).toISOString();
}

function buildFallbackContext(): MarketClickContext {
  const now = new Date();
  const metadata = extractMarketMetadata();

  return {
    marketId: metadata.marketId,
    marketTitle: metadata.marketTitle,
    marketQuestion: metadata.marketQuestion,
    marketSubtitle: metadata.marketSubtitle,
    marketRulesPrimary: metadata.marketRulesPrimary,
    clickedTimestamp: now.toISOString(),
    windowStart: addMinutes(now, -30),
    windowEnd: addMinutes(now, 30),
  };
}

function extractText(selectors: string[]): string | undefined {
  for (const selector of selectors) {
    const element = document.querySelector(selector);
    const text = element?.textContent?.replace(/\s+/g, " ").trim();
    if (text) {
      return text;
    }
  }

  return undefined;
}

function extractPriceFromText(): number | undefined {
  const candidates = Array.from(document.querySelectorAll("button, span, div")).slice(0, 120);

  for (const element of candidates) {
    const text = element.textContent?.trim();
    if (!text) {
      continue;
    }

    const centsMatch = text.match(/\b(\d{1,2})\s?[¢c]\b/);
    if (centsMatch) {
      return Number(centsMatch[1]) / 100;
    }
  }

  return undefined;
}

async function extractMarketContext(): Promise<MarketClickContext> {
  const now = new Date();
  const fallback = buildFallbackContext();
  const metadata = await resolveMarketMetadata();
  const title = metadata.marketTitle || extractText(["main h1", "h1", "[data-testid='market-title']"]) || fallback.marketTitle;
  const question =
    metadata.marketQuestion ||
    extractText(["main h2", "[data-testid='market-subtitle']", "[data-testid='market-question']"]) ||
    fallback.marketQuestion ||
    title;
  const price = extractPriceFromText();

  return {
    marketId: metadata.marketId || fallback.marketId,
    marketTitle: title,
    marketQuestion: question,
    marketSubtitle: metadata.marketSubtitle ?? fallback.marketSubtitle,
    marketRulesPrimary: metadata.marketRulesPrimary ?? fallback.marketRulesPrimary,
    clickedTimestamp: now.toISOString(),
    clickedPrice: price,
    windowStart: addMinutes(now, -30),
    windowEnd: addMinutes(now, 30),
    priceBefore: price !== undefined ? Math.max(0, Number((price - 0.08).toFixed(2))) : undefined,
    priceAfter: price,
  };
}

function formatDate(value: string): string {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatPercent(value: number | undefined): string {
  if (value === undefined) {
    return "n/a";
  }

  return `${Math.round(value * 100)}%`;
}

function summarizeMode(): { label: string; tone: string } {
  if (state.isLoading) {
    return {
      label: "Loading",
      tone: "loading",
    };
  }

  if (state.errorMessage) {
    return {
      label: "Last request failed",
      tone: "error",
    };
  }

  switch (state.resultSource) {
    case "live":
      return {
        label: "Localhost response",
        tone: "live",
      };
    case "fallback":
      return {
        label: "Fallback preview",
        tone: "mock",
      };
    default:
      return {
        label: "Mock preview",
        tone: "mock",
      };
  }
}

function createElement<K extends keyof HTMLElementTagNameMap>(
  tagName: K,
  options: {
    className?: string;
    text?: string;
    attributes?: Record<string, string>;
    onClick?: () => void;
  } = {},
): HTMLElementTagNameMap[K] {
  const element = document.createElement(tagName);

  if (options.className) {
    element.className = options.className;
  }

  if (options.text) {
    element.textContent = options.text;
  }

  if (options.attributes) {
    for (const [key, value] of Object.entries(options.attributes)) {
      element.setAttribute(key, value);
    }
  }

  if (options.onClick) {
    element.addEventListener("click", options.onClick);
  }

  return element;
}

function createLoadingPlaceholder(): HTMLDivElement {
  const wrapper = createElement("div", { className: "mme-loading-card" });

  for (let index = 0; index < 3; index += 1) {
    wrapper.append(createElement("div", { className: "mme-skeleton" }));
  }

  return wrapper;
}

function createResultCard(result: AttributionResponse): HTMLDivElement {
  const container = createElement("div");
  container.innerHTML = renderAttributionResponse(result).trim();

  const stack = container.firstElementChild;
  if (stack instanceof HTMLDivElement) {
    return stack;
  }

  return createElement("div", { className: "mme-result-stack" });
}

function render(): void {
  if (!panelApp) {
    return;
  }

  const status = summarizeMode();
  panelApp.replaceChildren();

  const launcher = createElement("button", {
    className: "mme-launcher",
    text: state.isOpen ? "Hide explainer" : "Open explainer",
    onClick: () => {
      state.isOpen = !state.isOpen;
      render();
    },
  });

  const shell = createElement("aside", {
    className: `mme-shell${state.isOpen ? " is-open" : ""}`,
  });

  const header = createElement("div", { className: "mme-header" });
  const titleGroup = createElement("div", { className: "mme-title-group" });
  titleGroup.append(
    createElement("span", { className: "mme-kicker", text: "Kalshi panel" }),
    createElement("h1", { className: "mme-title", text: "Market Move Explainer" }),
  );

  const statusBadge = createElement("span", {
    className: `mme-status mme-status-${status.tone}`,
    text: status.label,
  });

  header.append(titleGroup, statusBadge);
  shell.append(header);

  const subhead = createElement("p", {
    className: "mme-subhead",
    text: `Watching ${state.currentContext.marketId} • ${formatDate(state.currentContext.clickedTimestamp)}`,
  });
  shell.append(subhead);

  if (state.errorMessage) {
    shell.append(
      createElement("div", {
        className: "mme-error",
        text: state.errorMessage,
      }),
    );
  }

  if (state.noticeMessage) {
    const noticeCard = createElement("section", { className: "mme-card mme-card-accent" });
    noticeCard.append(
      createElement("span", { className: "mme-eyebrow", text: "Fallback" }),
      createElement("p", {
        className: "mme-card-note",
        text: state.noticeMessage,
      }),
    );
    shell.append(noticeCard);
  }

  const actions = createElement("div", { className: "mme-actions" });
  actions.append(
    createElement("button", {
      className: "mme-button mme-button-secondary",
      text: "Render mock",
      onClick: () => {
        void runAnalysis("mock");
      },
    }),
    createElement("button", {
      className: "mme-button mme-button-primary",
      text: "POST to localhost",
      onClick: () => {
        void runAnalysis("live");
      },
    }),
  );
  shell.append(actions);

  const endpointCard = createElement("section", { className: "mme-card mme-endpoint-card" });
  endpointCard.append(createElement("span", { className: "mme-eyebrow", text: "Local dev endpoint" }));

  const endpointRow = createElement("div", { className: "mme-endpoint-row" });
  const endpointInput = createElement("input", {
    className: "mme-endpoint-input",
    attributes: {
      type: "url",
      value: state.endpointUrl,
      placeholder: "http://127.0.0.1:8000/attribute_move",
    },
  });

  const saveButton = createElement("button", {
    className: "mme-button mme-button-ghost",
    text: "Save",
    onClick: () => {
      if (endpointInput instanceof HTMLInputElement) {
        void saveEndpoint(endpointInput.value);
      }
    },
  });

  endpointRow.append(endpointInput, saveButton);
  endpointCard.append(
    endpointRow,
    createElement("p", {
      className: "mme-endpoint-copy",
      text: "Mock mode renders locally. Chart clicks and the live button post the extracted market context to localhost first, then fall back to mock data if the backend is unavailable or incomplete.",
    }),
  );
  shell.append(endpointCard);

  if (state.isLoading) {
    const loadingOverlay = createElement("div", { className: "mme-loading-overlay" });
    loadingOverlay.append(
      createElement("div", { className: "mme-loading-spinner" }),
      createElement("p", { className: "mme-loading-text", text: "Analyzing market movement..." }),
    );

    if (state.result) {
      const staleWrapper = createElement("div", { className: "mme-stale-content" });
      staleWrapper.append(createResultCard(state.result));
      shell.append(staleWrapper, loadingOverlay);
    } else {
      shell.append(createLoadingPlaceholder());
    }
  } else if (state.result) {
    shell.append(createResultCard(state.result));
  } else {
    shell.append(
      createElement("div", {
        className: "mme-empty",
        text: "Click on a price movement in the chart to analyze what caused it.",
      }),
    );
  }

  panelApp.append(launcher, shell);
}

async function sendMessage<T extends RuntimeResponseMessage>(message: RuntimeRequestMessage): Promise<T> {
  return (await chrome.runtime.sendMessage(message)) as T;
}

async function saveEndpoint(endpointUrl: string): Promise<void> {
  const response = await sendMessage<UpdateSettingsResponseMessage | ErrorResponseMessage>({
    type: UPDATE_SETTINGS_REQUEST,
    payload: { endpointUrl } satisfies ExtensionSettings,
  });

  if (!response.ok) {
    state.errorMessage = response.error;
    render();
    return;
  }

  state.endpointUrl = response.data.endpointUrl;
  state.errorMessage = null;
  render();
}

async function runAnalysis(mode: RequestMode): Promise<void> {
  state.isLoading = true;
  state.errorMessage = null;
  state.noticeMessage = null;
  render();

  if (mode === "mock") {
    state.isLoading = false;
    state.result = buildMockAttributionResponse(state.currentContext);
    state.activeMode = "mock";
    state.resultSource = "mock";
    state.noticeMessage = null;
    render();
    return;
  }

  const response = await sendMessage<AttributeMoveResponseMessage | ErrorResponseMessage>({
    type: ATTRIBUTE_MOVE_REQUEST,
    payload: {
      context: state.currentContext,
      mode,
      endpointUrl: state.endpointUrl,
    },
  });

  if (!response.ok) {
    state.isLoading = false;
    state.errorMessage = response.error;
    state.noticeMessage = null;
    console.warn("[MME] Falling back to the existing panel state after an analysis error.", {
      mode,
      endpointUrl: state.endpointUrl,
      context: state.currentContext,
      error: response.error,
    });
    render();
    return;
  }

  state.isLoading = false;
  state.result = response.data;
  state.activeMode = response.meta.mode;
  state.resultSource =
    response.meta.mode === "live" ? (response.meta.mocked ? "fallback" : "live") : "mock";
  state.endpointUrl = response.meta.endpointUrl;
  state.noticeMessage = response.meta.fallbackReason ?? null;
  console.info("[MME] Rendering attribution response in the panel.", {
    mode: response.meta.mode,
    endpointUrl: response.meta.endpointUrl,
    marketId: response.data.primaryMarket.marketId,
  });
  render();
}

async function bootstrap(): Promise<void> {
  const response = await sendMessage<PanelBootstrapResponseMessage | ErrorResponseMessage>({
    type: PANEL_BOOTSTRAP_REQUEST,
  });

  if (!response.ok) {
    state.isLoading = false;
    state.errorMessage = response.error;
    console.warn("[MME] Bootstrap failed. Continuing with the local fallback context.", {
      error: response.error,
      fallbackContext: state.currentContext,
    });
    render();
    return;
  }

  state.endpointUrl = response.data.settings.endpointUrl;
  state.currentContext = {
    ...response.data.fallbackContext,
    ...(await extractMarketContext()),
  };
  render();
  await runAnalysis("mock");
}

function observeRouteChanges(): void {
  globalThis.setInterval(() => {
    if (globalThis.location.href === currentUrl) {
      return;
    }

    currentUrl = globalThis.location.href;
    void extractMarketContext()
      .then((context) => {
        state.currentContext = context;
        return runAnalysis("mock");
      })
      .catch(() => {
        state.currentContext = buildFallbackContext();
        return runAnalysis("mock");
      });
  }, URL_POLL_INTERVAL_MS);
}

function initializeMarketClickCapture(): void {
  initializeChartCapture({
    onContext: (context) => {
      state.currentContext = context;
      void runAnalysis("live");
    },
  });
}

function mountPanel(): void {
  if (document.getElementById(PANEL_HOST_ID)) {
    return;
  }

  const host = document.createElement("div");
  host.id = PANEL_HOST_ID;
  const shadowRoot = host.attachShadow({ mode: "open" });

  const stylesheet = document.createElement("link");
  stylesheet.rel = "stylesheet";
  stylesheet.href = chrome.runtime.getURL("ui/panel.css");

  panelApp = document.createElement("div");
  panelApp.className = "mme-app";

  shadowRoot.append(stylesheet, panelApp);
  document.documentElement.append(host);
  render();
}

mountPanel();
initializeMarketClickCapture();
observeRouteChanges();
void bootstrap();
