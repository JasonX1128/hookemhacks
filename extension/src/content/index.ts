import type { AttributionResponse, MarketClickContext } from "../shared/contracts";
import {
  ATTRIBUTE_MOVE_REQUEST,
  PANEL_BOOTSTRAP_REQUEST,
  PIPELINE_REFRESH_STATUS_REQUEST,
  PIPELINE_REFRESH_STOP_REQUEST,
  PIPELINE_REFRESH_TRIGGER_REQUEST,
  UPDATE_SETTINGS_REQUEST,
  type AttributeMoveResponseMessage,
  type ErrorResponseMessage,
  type ExtensionSettings,
  type PanelBootstrapResponseMessage,
  type PipelineRefreshStatus,
  type PipelineRefreshStatusResponseMessage,
  type PipelineRefreshStopResponseMessage,
  type PipelineRefreshTriggerResponseMessage,
  type RequestMode,
  type RuntimeRequestMessage,
  type RuntimeResponseMessage,
  type UpdateSettingsResponseMessage,
} from "../shared/messages";
import { renderAttributionResponse } from "../ui/renderers";
import { initializeChartCapture } from "./chartCapture";
import { extractMarketMetadata } from "./metadataExtractor";
import { extractVisibleMoveSummaryFromDom } from "./visibleMoveSummary";

const PANEL_HOST_ID = "market-move-explainer-root";
const NAVIGATION_DEBOUNCE_MS = 150;
const DOM_OBSERVER_DEBOUNCE_MS = 250;
const PIPELINE_STATUS_POLL_INTERVAL_MS = 15_000;
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
  pipelineRefresh: PipelineRefreshStatus | null;
  isTriggeringPipelineRefresh: boolean;
}

const state: PanelState = {
  isOpen: true,
  isLoading: true,
  errorMessage: null,
  noticeMessage: null,
  endpointUrl: "http://127.0.0.1:8000/attribute_move",
  currentContext: buildFallbackContext(),
  result: null,
  activeMode: "live",
  resultSource: "fallback",
  pipelineRefresh: null,
  isTriggeringPipelineRefresh: false,
};

let panelApp: HTMLDivElement | null = null;
let currentUrl = globalThis.location.href;
let pipelineStatusPollIntervalId: number | null = null;
let lastShellScrollTop = 0;
let shellElement: HTMLElement | null = null;
let pipelineNoticeTextElement: HTMLParagraphElement | null = null;
let pipelineRefreshButtonElement: HTMLButtonElement | null = null;
let navigationDebounceTimeoutId: number | null = null;
let domObserverDebounceTimeoutId: number | null = null;
let marketDomObserver: MutationObserver | null = null;
let lastObservedContextSignature = "";

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

function extractMarketContext(): MarketClickContext {
  const now = new Date();
  const fallback = buildFallbackContext();
  const metadata = extractMarketMetadata();
  const title = extractText(["main h1", "h1", "[data-testid='market-title']"]) ?? metadata.marketTitle;
  const question =
    extractText(["main h2", "[data-testid='market-subtitle']", "[data-testid='market-question']"]) ??
    metadata.marketQuestion ??
    title;
  const price = extractPriceFromText();

  return {
    marketId: metadata.marketId || fallback.marketId,
    marketTitle: title,
    marketQuestion: question,
    clickedTimestamp: now.toISOString(),
    clickedPrice: price,
    windowStart: addMinutes(now, -30),
    windowEnd: addMinutes(now, 30),
    priceBefore: price !== undefined ? Math.max(0, Number((price - 0.08).toFixed(2))) : undefined,
    priceAfter: price,
  };
}

function stablePriceValue(value: number | undefined): string {
  if (value === undefined || !Number.isFinite(value)) {
    return "";
  }
  return value.toFixed(2);
}

function marketContextSignature(context: MarketClickContext): string {
  return [
    context.marketId,
    context.marketTitle,
    context.marketQuestion,
    stablePriceValue(context.clickedPrice),
    stablePriceValue(context.priceBefore),
    stablePriceValue(context.priceAfter),
  ].join("|");
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

function formatMarketCountPhrase(count: number | null): string {
  if (count === null || count === undefined) {
    return "unknown";
  }
  return count.toLocaleString();
}

function buildPipelineRefreshNotice(status: PipelineRefreshStatus | null): {
  eyebrow: string;
  text: string;
} | null {
  if (!status) {
    return {
      eyebrow: "Pipeline refresh",
      text: "Discovered: unknown\nMetadata: unknown\nPairwise: unknown\nDate: unknown",
    };
  }

  const dateValue = status.running ? status.startedAt : status.finishedAt ?? status.startedAt;
  const datePhrase = dateValue ? formatDate(dateValue) : "unknown";

  return {
    eyebrow: "Pipeline refresh",
    text: [
      `Discovered: ${formatMarketCountPhrase(status.discoveredMarketCount)}`,
      `Metadata: ${formatMarketCountPhrase(status.artifactMarketCount ?? status.marketCount)}`,
      `Pairwise: ${formatMarketCountPhrase(status.pairwiseMarketCount)}`,
      `Date: ${datePhrase}`,
    ].join("\n"),
  };
}

function currentPipelineRefreshButtonLabel(): string {
  if (state.isTriggeringPipelineRefresh) {
    return state.pipelineRefresh?.running ? "Stopping refresh..." : "Starting refresh...";
  }
  return state.pipelineRefresh?.running ? "Stop refresh" : "Refresh pipeline";
}

function updatePipelineRefreshCard(): void {
  if (pipelineNoticeTextElement === null || pipelineRefreshButtonElement === null) {
    render();
    return;
  }

  const pipelineNotice = buildPipelineRefreshNotice(state.pipelineRefresh);
  if (pipelineNotice) {
    pipelineNoticeTextElement.textContent = pipelineNotice.text;
  }

  pipelineRefreshButtonElement.textContent = currentPipelineRefreshButtonLabel();
  pipelineRefreshButtonElement.disabled = state.isTriggeringPipelineRefresh;
}

function stopPipelineStatusPolling(): void {
  if (pipelineStatusPollIntervalId !== null) {
    globalThis.clearInterval(pipelineStatusPollIntervalId);
    pipelineStatusPollIntervalId = null;
  }
}

async function refreshPipelineStatus(): Promise<void> {
  const response = await sendMessage<PipelineRefreshStatusResponseMessage | ErrorResponseMessage>({
    type: PIPELINE_REFRESH_STATUS_REQUEST,
    payload: {
      endpointUrl: state.endpointUrl,
    },
  });

  if (!response.ok) {
    console.warn("[MME] Failed to refresh pipeline status.", {
      endpointUrl: state.endpointUrl,
      error: response.error,
    });
    return;
  }

  state.pipelineRefresh = response.data;
  if (!state.pipelineRefresh?.running) {
    stopPipelineStatusPolling();
  } else {
    ensurePipelineStatusPolling();
  }
  updatePipelineRefreshCard();
}

function ensurePipelineStatusPolling(): void {
  if (!state.pipelineRefresh?.running || pipelineStatusPollIntervalId !== null) {
    return;
  }

  pipelineStatusPollIntervalId = globalThis.setInterval(() => {
    void refreshPipelineStatus();
  }, PIPELINE_STATUS_POLL_INTERVAL_MS);
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
  const visibleSummary = extractVisibleMoveSummaryFromDom(
    state.currentContext.marketTitle,
    state.currentContext.marketQuestion,
    state.currentContext.clickedTimestamp,
  );
  const container = createElement("div");
  container.innerHTML = renderAttributionResponse(result, visibleSummary).trim();

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

  if (shellElement) {
    lastShellScrollTop = shellElement.scrollTop;
  }
  shellElement = null;
  pipelineNoticeTextElement = null;
  pipelineRefreshButtonElement = null;

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
  shellElement = shell;

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

  const pipelineNotice = buildPipelineRefreshNotice(state.pipelineRefresh);
  if (pipelineNotice) {
    const pipelineNoticeCard = createElement("section", { className: "mme-card mme-card-accent" });
    const pipelineActions = createElement("div", { className: "mme-actions" });
    const pipelineRefreshButton = createElement("button", {
      className: "mme-button mme-button-ghost",
      text: currentPipelineRefreshButtonLabel(),
      attributes: { type: "button" },
      onClick: () => {
        if (state.isTriggeringPipelineRefresh) {
          return;
        }
        if (state.pipelineRefresh?.running) {
          void stopPipelineRefresh();
        } else {
          void triggerPipelineRefresh();
        }
      },
    });
    if (state.isTriggeringPipelineRefresh) {
      pipelineRefreshButton.disabled = true;
    }
    pipelineRefreshButtonElement = pipelineRefreshButton;
    pipelineActions.append(pipelineRefreshButton);
    const pipelineNoticeText = createElement("p", {
      className: "mme-card-note mme-card-note-multiline",
      text: pipelineNotice.text,
    });
    pipelineNoticeTextElement = pipelineNoticeText;
    pipelineNoticeCard.append(
      createElement("span", { className: "mme-eyebrow", text: pipelineNotice.eyebrow }),
      pipelineNoticeText,
      pipelineActions,
    );
    shell.append(pipelineNoticeCard);
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

  if (state.isLoading && !state.result) {
    shell.append(createLoadingPlaceholder());
  } else if (state.result) {
    shell.append(createResultCard(state.result));
  } else {
    shell.append(
      createElement("div", {
        className: "mme-empty",
        text: "No attribution yet. Render the mock preview or send the current extracted market context to localhost.",
      }),
    );
  }

  panelApp.append(launcher, shell);
  shell.scrollTop = lastShellScrollTop;
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

async function triggerPipelineRefresh(): Promise<void> {
  state.isTriggeringPipelineRefresh = true;
  state.errorMessage = null;
  updatePipelineRefreshCard();

  const response = await sendMessage<PipelineRefreshTriggerResponseMessage | ErrorResponseMessage>({
    type: PIPELINE_REFRESH_TRIGGER_REQUEST,
    payload: {
      endpointUrl: state.endpointUrl,
    },
  });

  state.isTriggeringPipelineRefresh = false;

  if (!response.ok) {
    state.errorMessage = response.error;
    render();
    return;
  }

  state.pipelineRefresh = response.data;
  ensurePipelineStatusPolling();
  updatePipelineRefreshCard();
}

async function stopPipelineRefresh(): Promise<void> {
  state.isTriggeringPipelineRefresh = true;
  state.errorMessage = null;
  updatePipelineRefreshCard();

  const response = await sendMessage<PipelineRefreshStopResponseMessage | ErrorResponseMessage>({
    type: PIPELINE_REFRESH_STOP_REQUEST,
    payload: {
      endpointUrl: state.endpointUrl,
    },
  });

  state.isTriggeringPipelineRefresh = false;

  if (!response.ok) {
    state.errorMessage = response.error;
    render();
    return;
  }

  state.pipelineRefresh = response.data;
  stopPipelineStatusPolling();
  updatePipelineRefreshCard();
}

async function runAnalysis(mode: RequestMode): Promise<void> {
  state.isLoading = true;
  state.errorMessage = null;
  state.noticeMessage = null;
  render();

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
  state.pipelineRefresh = response.data.pipelineRefresh;
  state.currentContext = {
    ...response.data.fallbackContext,
    ...extractMarketContext(),
  };
  lastObservedContextSignature = marketContextSignature(state.currentContext);
  ensurePipelineStatusPolling();
  render();
  await runAnalysis("live");
}

function handlePotentialNavigation(): void {
  if (globalThis.location.href === currentUrl) {
    return;
  }

  currentUrl = globalThis.location.href;
  state.currentContext = extractMarketContext();
  lastObservedContextSignature = marketContextSignature(state.currentContext);
  void runAnalysis("live");
}

function observeRouteChanges(): void {
  const scheduleNavigationCheck = (): void => {
    if (navigationDebounceTimeoutId !== null) {
      globalThis.clearTimeout(navigationDebounceTimeoutId);
    }
    navigationDebounceTimeoutId = globalThis.setTimeout(() => {
      navigationDebounceTimeoutId = null;
      handlePotentialNavigation();
    }, NAVIGATION_DEBOUNCE_MS);
  };

  const originalPushState = history.pushState.bind(history);
  history.pushState = ((...args: Parameters<History["pushState"]>) => {
    originalPushState(...args);
    scheduleNavigationCheck();
  }) as History["pushState"];

  const originalReplaceState = history.replaceState.bind(history);
  history.replaceState = ((...args: Parameters<History["replaceState"]>) => {
    originalReplaceState(...args);
    scheduleNavigationCheck();
  }) as History["replaceState"];

  globalThis.addEventListener("popstate", scheduleNavigationCheck);
  globalThis.addEventListener("hashchange", scheduleNavigationCheck);
}

function isNodeWithinPanelHost(node: Node | null): boolean {
  if (!node) {
    return false;
  }
  if (node instanceof Element) {
    return node.id === PANEL_HOST_ID || Boolean(node.closest(`#${PANEL_HOST_ID}`));
  }
  const parent = node.parentElement;
  return parent ? parent.id === PANEL_HOST_ID || Boolean(parent.closest(`#${PANEL_HOST_ID}`)) : false;
}

function refreshForObservedMarketChange(): void {
  const nextContext = extractMarketContext();
  const nextSignature = marketContextSignature(nextContext);
  if (nextSignature === lastObservedContextSignature) {
    return;
  }
  lastObservedContextSignature = nextSignature;
  state.currentContext = nextContext;
  if (!state.isLoading) {
    void runAnalysis("live");
  }
}

function observeMarketDomChanges(): void {
  if (marketDomObserver !== null) {
    marketDomObserver.disconnect();
  }

  marketDomObserver = new MutationObserver((mutations) => {
    const relevantMutationSeen = mutations.some((mutation) => {
      if (!isNodeWithinPanelHost(mutation.target)) {
        return true;
      }
      return Array.from(mutation.addedNodes).some((node) => !isNodeWithinPanelHost(node))
        || Array.from(mutation.removedNodes).some((node) => !isNodeWithinPanelHost(node));
    });

    if (!relevantMutationSeen) {
      return;
    }

    if (domObserverDebounceTimeoutId !== null) {
      globalThis.clearTimeout(domObserverDebounceTimeoutId);
    }
    domObserverDebounceTimeoutId = globalThis.setTimeout(() => {
      domObserverDebounceTimeoutId = null;
      refreshForObservedMarketChange();
    }, DOM_OBSERVER_DEBOUNCE_MS);
  });

  const root = document.body ?? document.documentElement;
  marketDomObserver.observe(root, {
    childList: true,
    subtree: true,
    characterData: true,
    attributes: true,
    attributeFilter: ["content", "data-market-id", "data-market-ticker", "data-ticker"],
  });
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
observeMarketDomChanges();
void bootstrap();
