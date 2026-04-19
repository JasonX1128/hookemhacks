import {
  isAttributionResponse,
  type AttributionResponse,
  type CatalystCandidate,
  type MarketClickContext,
  type MoveDirection,
  type MoveSummary,
  type RelatedMarket,
  type RelatedMarketStatus,
} from "./contracts";
import { buildMockAttributionResponse } from "./fixtures/mockAttributionResponse";
import type { PipelineRefreshStatus } from "./messages";

export const DEFAULT_ENDPOINT_URL = "http://127.0.0.1:8000/attribute_move";
const REQUEST_TIMEOUT_MS = 12_000;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function readString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

function readNumber(record: Record<string, unknown>, key: string): number | undefined {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readBoolean(record: Record<string, unknown>, key: string): boolean | undefined {
  const value = record[key];
  return typeof value === "boolean" ? value : undefined;
}

function isMoveDirection(value: unknown): value is MoveDirection {
  return value === "up" || value === "down" || value === "flat";
}

function isRelatedMarketStatus(value: unknown): value is RelatedMarketStatus {
  return value === "normal" || value === "possibly_lagging" || value === "divergent";
}

function isCatalystCandidate(value: unknown): value is CatalystCandidate {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.title === "string" &&
    typeof value.timestamp === "string" &&
    typeof value.source === "string"
  );
}

function isRelatedMarket(value: unknown): value is RelatedMarket {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.marketId === "string" &&
    typeof value.title === "string" &&
    Array.isArray(value.relationTypes) &&
    value.relationTypes.every((relation) => typeof relation === "string") &&
    typeof value.relationStrength === "number"
  );
}

function coerceMoveSummary(value: unknown, fallback: MoveSummary): MoveSummary {
  if (!isRecord(value)) {
    return fallback;
  }

  return {
    moveMagnitude: readNumber(value, "moveMagnitude") ?? fallback.moveMagnitude,
    moveDirection: isMoveDirection(value.moveDirection) ? value.moveDirection : fallback.moveDirection,
    jumpScore: readNumber(value, "jumpScore") ?? fallback.jumpScore,
  };
}

function coercePrimaryMarket(value: unknown, fallback: MarketClickContext): MarketClickContext {
  if (!isRecord(value)) {
    return fallback;
  }

  return {
    marketId: readString(value, "marketId") ?? fallback.marketId,
    marketTitle: readString(value, "marketTitle") ?? fallback.marketTitle,
    marketQuestion: readString(value, "marketQuestion") ?? fallback.marketQuestion,
    clickedTimestamp: readString(value, "clickedTimestamp") ?? fallback.clickedTimestamp,
    clickedPrice: readNumber(value, "clickedPrice") ?? fallback.clickedPrice,
    windowStart: readString(value, "windowStart") ?? fallback.windowStart,
    windowEnd: readString(value, "windowEnd") ?? fallback.windowEnd,
    priceBefore: readNumber(value, "priceBefore") ?? fallback.priceBefore,
    priceAfter: readNumber(value, "priceAfter") ?? fallback.priceAfter,
  };
}

function coerceCandidateArray(value: unknown, fallback: CatalystCandidate[]): CatalystCandidate[] {
  if (!Array.isArray(value)) {
    return fallback;
  }

  const validCandidates = value.filter(isCatalystCandidate);
  if (validCandidates.length > 0 || value.length === 0) {
    return validCandidates;
  }

  return fallback;
}

function coerceRelatedMarketArray(value: unknown, fallback: RelatedMarket[]): RelatedMarket[] {
  if (!Array.isArray(value)) {
    return fallback;
  }

  const validMarkets = value
    .filter(isRelatedMarket)
    .map((market) => ({
      ...market,
      status: isRelatedMarketStatus(market.status) ? market.status : undefined,
    }));
  if (validMarkets.length > 0 || value.length === 0) {
    return validMarkets;
  }

  return fallback;
}

export function coerceAttributionResponse(
  payload: unknown,
  context: MarketClickContext,
): AttributionResponse | null {
  if (!isRecord(payload)) {
    return null;
  }

  const fallback = buildMockAttributionResponse(context);
  const topCatalyst = payload.topCatalyst;

  return {
    primaryMarket: coercePrimaryMarket(payload.primaryMarket, fallback.primaryMarket),
    moveSummary: coerceMoveSummary(payload.moveSummary, fallback.moveSummary),
    topCatalyst:
      topCatalyst === null || topCatalyst === undefined
        ? undefined
        : isCatalystCandidate(topCatalyst)
          ? topCatalyst
          : fallback.topCatalyst,
    alternativeCatalysts: coerceCandidateArray(payload.alternativeCatalysts, fallback.alternativeCatalysts),
    confidence: readNumber(payload, "confidence") ?? fallback.confidence,
    evidence: coerceCandidateArray(payload.evidence, fallback.evidence),
    relatedMarkets: coerceRelatedMarketArray(payload.relatedMarkets, fallback.relatedMarkets),
  };
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeoutId = globalThis.setTimeout(() => reject(new Error("Backend request timed out.")), timeoutMs);

    promise
      .then((value) => {
        globalThis.clearTimeout(timeoutId);
        resolve(value);
      })
      .catch((error) => {
        globalThis.clearTimeout(timeoutId);
        reject(error);
      });
  });
}

export function normalizeEndpointUrl(value: string | undefined): string {
  if (!value) {
    return DEFAULT_ENDPOINT_URL;
  }

  try {
    return new URL(value).toString();
  } catch {
    return DEFAULT_ENDPOINT_URL;
  }
}

export async function postAttributionRequest(
  context: MarketClickContext,
  endpointUrl: string = DEFAULT_ENDPOINT_URL,
): Promise<AttributionResponse> {
  const normalizedEndpointUrl = normalizeEndpointUrl(endpointUrl);
  console.info("[MME] Posting attribution request to localhost.", {
    endpointUrl: normalizedEndpointUrl,
    marketId: context.marketId,
    clickedTimestamp: context.clickedTimestamp,
  });

  let response: Response;
  try {
    response = await withTimeout(
      fetch(normalizedEndpointUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(context),
      }),
      REQUEST_TIMEOUT_MS,
    );
  } catch (error) {
    console.error("[MME] Attribution request failed before a response was received.", {
      endpointUrl: normalizedEndpointUrl,
      context,
      error,
    });
    throw error;
  }

  if (!response.ok) {
    const errorBody = await response.text().catch(() => "");
    console.error("[MME] Backend returned a non-OK response.", {
      endpointUrl: normalizedEndpointUrl,
      status: response.status,
      body: errorBody,
    });
    throw new Error(`Backend returned ${response.status}`);
  }

  const payload: unknown = await response.json();
  const normalizedPayload = coerceAttributionResponse(payload, context);

  if (!normalizedPayload) {
    console.error("[MME] Backend response did not match AttributionResponse.", {
      endpointUrl: normalizedEndpointUrl,
      payload,
    });
    throw new Error("Backend response did not match AttributionResponse.");
  }

  if (!isAttributionResponse(payload)) {
    console.warn("[MME] Backend response was partially normalized with mock defaults.", {
      endpointUrl: normalizedEndpointUrl,
      marketId: context.marketId,
      payload,
    });
  }

  return normalizedPayload;
}

function deriveBackendUrl(endpointUrl: string, pathname: string): string {
  const url = new URL(normalizeEndpointUrl(endpointUrl));
  url.pathname = pathname;
  url.search = "";
  url.hash = "";
  return url.toString();
}

function coercePipelineRefreshStatus(payload: unknown): PipelineRefreshStatus | null {
  if (!isRecord(payload)) {
    return null;
  }

  const command = Array.isArray(payload.command) ? payload.command.filter((item) => typeof item === "string") : [];
  return {
    status: readString(payload, "status") ?? "unknown",
    running: readBoolean(payload, "running") ?? false,
    started: readBoolean(payload, "started") ?? false,
    command,
    configPath: readString(payload, "config_path") ?? "",
    logPath: readString(payload, "log_path") ?? "",
    pid: readNumber(payload, "pid") ?? null,
    startedAt: readString(payload, "started_at") ?? null,
    finishedAt: readString(payload, "finished_at") ?? null,
    exitCode: readNumber(payload, "exit_code") ?? null,
    marketCount: readNumber(payload, "market_count") ?? null,
    artifactMarketCount: readNumber(payload, "artifact_market_count") ?? null,
    discoveredMarketCount: readNumber(payload, "discovered_market_count") ?? null,
    pairwiseMarketCount: readNumber(payload, "pairwise_market_count") ?? null,
    progressStatus: readString(payload, "progress_status") ?? null,
    progressMessage: readString(payload, "progress_message") ?? null,
    reason: readString(payload, "reason") ?? null,
  };
}

async function requestBackendJson(
  url: string,
  init: RequestInit,
  errorPrefix: string,
): Promise<unknown> {
  let response: Response;
  try {
    response = await withTimeout(fetch(url, init), REQUEST_TIMEOUT_MS);
  } catch (error) {
    console.error("[MME] Backend request failed before a response was received.", { url, error });
    throw error;
  }

  if (!response.ok) {
    const errorBody = await response.text().catch(() => "");
    console.error("[MME] Backend returned a non-OK response.", {
      url,
      status: response.status,
      body: errorBody,
    });
    throw new Error(`${errorPrefix} returned ${response.status}`);
  }

  return response.json();
}

export async function postStartupPipelineRefresh(
  endpointUrl: string = DEFAULT_ENDPOINT_URL,
): Promise<PipelineRefreshStatus | null> {
  const startupUrl = deriveBackendUrl(endpointUrl, "/pipeline/startup_refresh");
  const payload = await requestBackendJson(
    startupUrl,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    },
    "Pipeline startup refresh",
  );
  return coercePipelineRefreshStatus(payload);
}

export async function postStopPipelineRefresh(
  endpointUrl: string = DEFAULT_ENDPOINT_URL,
): Promise<PipelineRefreshStatus | null> {
  const stopUrl = deriveBackendUrl(endpointUrl, "/pipeline/stop_refresh");
  const payload = await requestBackendJson(
    stopUrl,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    },
    "Pipeline stop refresh",
  );
  return coercePipelineRefreshStatus(payload);
}

export async function fetchStartupPipelineStatus(
  endpointUrl: string = DEFAULT_ENDPOINT_URL,
): Promise<PipelineRefreshStatus | null> {
  const statusUrl = deriveBackendUrl(endpointUrl, "/pipeline/startup_status");
  const payload = await requestBackendJson(
    statusUrl,
    {
      method: "GET",
      headers: { Accept: "application/json" },
    },
    "Pipeline startup status",
  );
  return coercePipelineRefreshStatus(payload);
}
