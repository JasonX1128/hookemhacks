import {
  fetchStartupPipelineStatus,
  postAttributionRequest,
  postStopPipelineRefresh,
  postStartupPipelineRefresh,
  normalizeEndpointUrl,
  DEFAULT_ENDPOINT_URL,
} from "../shared/api";
import { buildMockAttributionResponse } from "../shared/fixtures/mockAttributionResponse";
import { mockMarketClickContext } from "../shared/fixtures/mockMarketClickContext";
import {
  ATTRIBUTE_MOVE_REQUEST,
  PANEL_BOOTSTRAP_REQUEST,
  PIPELINE_REFRESH_STATUS_REQUEST,
  PIPELINE_REFRESH_STOP_REQUEST,
  PIPELINE_REFRESH_TRIGGER_REQUEST,
  UPDATE_SETTINGS_REQUEST,
  type AttributeMoveRequestMessage,
  type ErrorResponseMessage,
  type ExtensionSettings,
  type PipelineRefreshStatusRequestMessage,
  type PipelineRefreshStopRequestMessage,
  type PipelineRefreshTriggerRequestMessage,
  type RuntimeRequestMessage,
  type RuntimeResponseMessage,
  type UpdateSettingsRequestMessage,
} from "../shared/messages";

const SETTINGS_STORAGE_KEY = "market-move-explainer/settings";
const CONTENT_SCRIPT_ID = "market-move-explainer-kalshi-panel";
const KALSHI_MATCHES = ["https://kalshi.com/*", "https://*.kalshi.com/*"];
let contentScriptRegistrationPromise: Promise<void> | null = null;

function isDuplicateScriptIdError(error: unknown): boolean {
  return error instanceof Error && error.message.includes(`Duplicate script ID '${CONTENT_SCRIPT_ID}'`);
}

async function ensureContentScriptRegistered(): Promise<void> {
  if (contentScriptRegistrationPromise) {
    return contentScriptRegistrationPromise;
  }

  contentScriptRegistrationPromise = (async () => {
    const registeredScripts = await chrome.scripting.getRegisteredContentScripts({
      ids: [CONTENT_SCRIPT_ID],
    });

    if (registeredScripts.length > 0) {
      return;
    }

    try {
      await chrome.scripting.registerContentScripts([
        {
          id: CONTENT_SCRIPT_ID,
          js: ["content/index.js"],
          matches: KALSHI_MATCHES,
          runAt: "document_idle",
          persistAcrossSessions: true,
        },
      ]);
    } catch (error) {
      if (isDuplicateScriptIdError(error)) {
        console.warn("[MME] Content script registration raced with another startup path; continuing.", {
          contentScriptId: CONTENT_SCRIPT_ID,
        });
        return;
      }

      throw error;
    }
  })();

  try {
    await contentScriptRegistrationPromise;
  } finally {
    contentScriptRegistrationPromise = null;
  }
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

async function handlePipelineRefreshStatus(
  message: PipelineRefreshStatusRequestMessage,
): Promise<RuntimeResponseMessage> {
  const settings = await loadSettings();
  const endpointUrl = normalizeEndpointUrl(message.payload?.endpointUrl ?? settings.endpointUrl);
  const status = await fetchStartupPipelineStatus(endpointUrl);
  return {
    ok: true,
    type: PIPELINE_REFRESH_STATUS_REQUEST,
    data: status,
  };
}

async function handlePipelineRefreshTrigger(
  message: PipelineRefreshTriggerRequestMessage,
): Promise<RuntimeResponseMessage> {
  const settings = await loadSettings();
  const endpointUrl = normalizeEndpointUrl(message.payload?.endpointUrl ?? settings.endpointUrl);
  const status = await postStartupPipelineRefresh(endpointUrl);
  return {
    ok: true,
    type: PIPELINE_REFRESH_TRIGGER_REQUEST,
    data: status,
  };
}

async function handlePipelineRefreshStop(
  message: PipelineRefreshStopRequestMessage,
): Promise<RuntimeResponseMessage> {
  const settings = await loadSettings();
  const endpointUrl = normalizeEndpointUrl(message.payload?.endpointUrl ?? settings.endpointUrl);
  const status = await postStopPipelineRefresh(endpointUrl);
  return {
    ok: true,
    type: PIPELINE_REFRESH_STOP_REQUEST,
    data: status,
  };
}

async function handleMessage(message: RuntimeRequestMessage): Promise<RuntimeResponseMessage> {
  switch (message.type) {
    case PANEL_BOOTSTRAP_REQUEST: {
      const settings = await loadSettings();
      const pipelineRefresh = await fetchStartupPipelineStatus(settings.endpointUrl).catch((error) => {
        console.warn("[MME] Could not load pipeline refresh status during bootstrap.", { error });
        return null;
      });
      return {
        ok: true,
        type: PANEL_BOOTSTRAP_REQUEST,
        data: {
          settings,
          fallbackContext: mockMarketClickContext,
          pipelineRefresh,
        },
      };
    }
    case ATTRIBUTE_MOVE_REQUEST:
      return handleAttributeRequest(message);
    case UPDATE_SETTINGS_REQUEST:
      return handleSettingsUpdate(message);
    case PIPELINE_REFRESH_STATUS_REQUEST:
      return handlePipelineRefreshStatus(message);
    case PIPELINE_REFRESH_TRIGGER_REQUEST:
      return handlePipelineRefreshTrigger(message);
    case PIPELINE_REFRESH_STOP_REQUEST:
      return handlePipelineRefreshStop(message);
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
