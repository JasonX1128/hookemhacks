import type { AttributionResponse } from "../shared/contracts";
import { renderAttributionResponse } from "./renderers";

const ROOT_ID = "kalshify-root";
const BODY_ID = "kalshify-body";
const STATUS_ID = "kalshify-status";
const CLOSE_BUTTON_ID = "kalshify-close";
const MOCK_BUTTON_ID = "mme-mock-run";

type StatusTone = "mock" | "loading" | "live" | "error";

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function ensureStyles(): void {
  const href = chrome.runtime.getURL("ui/panel.css");
  if (document.querySelector(`link[href="${href}"]`)) {
    return;
  }

  const link = document.createElement("link");
  link.rel = "stylesheet";
  link.href = href;
  document.head.appendChild(link);
}

function ensureRoot(): HTMLElement {
  const existingRoot = document.getElementById(ROOT_ID);
  if (existingRoot) {
    return existingRoot;
  }

  const root = document.createElement("aside");
  root.id = ROOT_ID;
  root.style.display = "none";
  root.innerHTML = `
    <div class="mme-shell">
      <header class="mme-header">
        <div class="mme-title-group">
          <span class="mme-kicker">Kalshi panel</span>
          <h1 class="mme-title">Kalshify</h1>
        </div>
        <div class="mme-header-actions">
          <span id="${STATUS_ID}" class="mme-status mme-status-mock">Ready</span>
          <button id="${CLOSE_BUTTON_ID}" class="mme-button mme-button-ghost" type="button">Hide</button>
        </div>
      </header>
      <p class="mme-subhead">Compact summary, likely catalyst, evidence, related markets, and what is worth checking next.</p>
      <div id="${BODY_ID}" class="mme-panel-body"></div>
    </div>
  `;

  document.body.appendChild(root);

  root.querySelector<HTMLButtonElement>(`#${CLOSE_BUTTON_ID}`)?.addEventListener("click", () => {
    root.style.display = "none";
  });

  return root;
}

export class MarketMovePanel {
  private readonly root: HTMLElement;
  private readonly body: HTMLElement;
  private readonly status: HTMLElement;

  constructor() {
    ensureStyles();
    this.root = ensureRoot();
    this.body = this.root.querySelector<HTMLElement>(`#${BODY_ID}`) ?? this.root;
    this.status = this.root.querySelector<HTMLElement>(`#${STATUS_ID}`) ?? this.root;
  }

  open(): void {
    this.root.style.display = "block";
  }

  private setStatus(message: string, tone: StatusTone): void {
    this.status.textContent = message;
    this.status.className = `mme-status mme-status-${tone}`;
  }

  private bindMockButton(onMockClick: () => void): void {
    this.body.querySelector<HTMLButtonElement>(`#${MOCK_BUTTON_ID}`)?.addEventListener("click", onMockClick);
  }

  showIdle(onMockClick: () => void): void {
    this.open();
    this.setStatus("Waiting for click", "mock");
    this.body.innerHTML = `
      <section class="mme-empty-state mme-card">
        <span class="mme-eyebrow">Empty state</span>
        <h2 class="mme-section-title">Click a market move to fill this panel</h2>
        <p class="mme-empty-copy">The panel is ready to show the move summary, likely catalyst, evidence, related markets, and anything worth checking.</p>
        <button id="${MOCK_BUTTON_ID}" class="mme-inline-button" type="button">Preview with mock data</button>
      </section>
    `;
    this.bindMockButton(onMockClick);
  }

  showLoading(contextLabel: string): void {
    this.open();
    this.setStatus("Analyzing", "loading");
    this.body.innerHTML = `
      <section class="mme-card mme-loading-card">
        <span class="mme-eyebrow">Loading</span>
        <h2 class="mme-section-title">Scanning ${escapeHtml(contextLabel)}</h2>
        <p class="mme-empty-copy">Checking nearby headlines, scheduled events, and cross-market reactions.</p>
        <div class="mme-skeleton"></div>
        <div class="mme-skeleton mme-skeleton-wide"></div>
        <div class="mme-skeleton"></div>
        <div class="mme-skeleton mme-skeleton-short"></div>
      </section>
    `;
  }

  showError(message: string, onMockClick: () => void): void {
    this.open();
    this.setStatus("Request failed", "error");
    this.body.innerHTML = `
      <section class="mme-card mme-error">
        <span class="mme-eyebrow">Error</span>
        <h2 class="mme-section-title">Could not load this move</h2>
        <p class="mme-empty-copy">${escapeHtml(message)}</p>
        <button id="${MOCK_BUTTON_ID}" class="mme-inline-button" type="button">Retry with mock data</button>
      </section>
    `;
    this.bindMockButton(onMockClick);
  }

  showResponse(response: AttributionResponse): void {
    this.open();
    this.setStatus(`Updated ${formatTimestamp(response.primaryMarket.clickedTimestamp)}`, "live");
    this.body.innerHTML = renderAttributionResponse(response);
  }
}
