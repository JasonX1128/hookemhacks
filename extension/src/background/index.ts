import { postAttributionRequest, normalizeEndpointUrl, DEFAULT_ENDPOINT_URL } from "../shared/api";
import { buildMockAttributionResponse } from "../shared/fixtures/mockAttributionResponse";
import { mockMarketClickContext } from "../shared/fixtures/mockMarketClickContext";
import {
  ATTRIBUTE_MOVE_REQUEST,
  PANEL_BOOTSTRAP_REQUEST,
  UPDATE_SETTINGS_REQUEST,
  type AttributeMoveRequestMessage,
  type ErrorResponseMessage,
  type ExtensionSettings,
  type RuntimeRequestMessage,
  type RuntimeResponseMessage,
  type UpdateSettingsRequestMessage,
} from "../shared/messages";

const SETTINGS_STORAGE_KEY = "market-move-explainer/settings";
const CONTENT_SCRIPT_ID = "market-move-explainer-kalshi-panel";
const KALSHI_MATCHES = ["https://kalshi.com/*", "https://*.kalshi.com/*"];

async function ensureContentScriptRegistered(): Promise<void> {
  const registeredScripts = await chrome.scripting.getRegisteredContentScripts({
    ids: [CONTENT_SCRIPT_ID],
  });

  if (registeredScripts.length > 0) {
    return;
  }

  await chrome.scripting.registerContentScripts([
    {
      id: CONTENT_SCRIPT_ID,
      js: ["content/index.js"],
      matches: KALSHI_MATCHES,
      runAt: "document_idle",
      persistAcrossSessions: true,
    },
  ]);
}

async function loadSettings(): Promise<ExtensionSettings> {
  const stored = await chrome.storage.local.get(SETTINGS_STORAGE_KEY);
  const candidate = stored[SETTINGS_STORAGE_KEY] as Partial<ExtensionSettings> | undefined;

  return {
    endpointUrl: normalizeEndpointUrl(candidate?.endpointUrl),
  };
}

async function saveSettings(payload: Partial<ExtensionSettings>): Promise<ExtensionSettings> {
  const current = await loadSettings();
  const next: ExtensionSettings = {
    endpointUrl: normalizeEndpointUrl(payload.endpointUrl ?? current.endpointUrl ?? DEFAULT_ENDPOINT_URL),
  };

  await chrome.storage.local.set({
    [SETTINGS_STORAGE_KEY]: next,
  });

  return next;
}

function buildError(type: RuntimeRequestMessage["type"], error: unknown): ErrorResponseMessage {
  const message = error instanceof Error ? error.message : "Unknown extension error.";
  console.error("[MME] Runtime request failed.", { type, error });
  return {
    ok: false,
    type,
    error: message,
  };
}

async function handleAttributeRequest(
  message: AttributeMoveRequestMessage,
): Promise<RuntimeResponseMessage> {
  const settings = await loadSettings();
  const endpointUrl = normalizeEndpointUrl(message.payload.endpointUrl ?? settings.endpointUrl);

  if (message.payload.mode === "mock") {
    return {
      ok: true,
      type: ATTRIBUTE_MOVE_REQUEST,
      data: buildMockAttributionResponse(message.payload.context),
      meta: {
        mode: "mock",
        endpointUrl,
        mocked: true,
      },
    };
  }

  try {
    const data = await postAttributionRequest(message.payload.context, endpointUrl);
    return {
      ok: true,
      type: ATTRIBUTE_MOVE_REQUEST,
      data,
      meta: {
        mode: "live",
        endpointUrl,
        mocked: false,
      },
    };
  } catch (error) {
    const fallbackReason = error instanceof Error ? error.message : "Live attribution request failed.";
    console.warn("[MME] Live attribution failed. Returning the mock fallback response instead.", {
      endpointUrl,
      context: message.payload.context,
      error,
    });

    return {
      ok: true,
      type: ATTRIBUTE_MOVE_REQUEST,
      data: buildMockAttributionResponse(message.payload.context),
      meta: {
        mode: "live",
        endpointUrl,
        mocked: true,
        fallbackReason,
      },
    };
  }
}

async function handleSettingsUpdate(
  message: UpdateSettingsRequestMessage,
): Promise<RuntimeResponseMessage> {
  const settings = await saveSettings(message.payload);
  return {
    ok: true,
    type: UPDATE_SETTINGS_REQUEST,
    data: settings,
  };
}

async function handleMessage(message: RuntimeRequestMessage): Promise<RuntimeResponseMessage> {
  switch (message.type) {
    case PANEL_BOOTSTRAP_REQUEST:
      return {
        ok: true,
        type: PANEL_BOOTSTRAP_REQUEST,
        data: {
          settings: await loadSettings(),
          fallbackContext: mockMarketClickContext,
        },
      };
    case ATTRIBUTE_MOVE_REQUEST:
      return handleAttributeRequest(message);
    case UPDATE_SETTINGS_REQUEST:
      return handleSettingsUpdate(message);
    default:
      return buildError(message.type, new Error("Unsupported runtime message."));
  }
}

chrome.runtime.onInstalled.addListener(() => {
  void ensureContentScriptRegistered();
});

chrome.runtime.onStartup.addListener(() => {
  void ensureContentScriptRegistered();
});

chrome.runtime.onMessage.addListener((message: RuntimeRequestMessage, _sender, sendResponse) => {
  void handleMessage(message)
    .then((response) => {
      sendResponse(response);
    })
    .catch((error) => {
      sendResponse(buildError(message.type, error));
    });

  return true;
});

void ensureContentScriptRegistered();
