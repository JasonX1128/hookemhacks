import type { AttributionResponse, AttributionSynthesisResponse, MarketClickContext } from "./contracts";

export const PANEL_BOOTSTRAP_REQUEST = "kalshify/panel-bootstrap";
export const ATTRIBUTE_MOVE_REQUEST = "kalshify/attribute-move";
export const ATTRIBUTE_MOVE_SYNTHESIS_REQUEST = "kalshify/attribute-move-synthesis";
export const UPDATE_SETTINGS_REQUEST = "kalshify/update-settings";
export const PIPELINE_REFRESH_STATUS_REQUEST = "kalshify/pipeline-refresh-status";
export const PIPELINE_REFRESH_TRIGGER_REQUEST = "kalshify/pipeline-refresh-trigger";
export const PIPELINE_REFRESH_STOP_REQUEST = "kalshify/pipeline-refresh-stop";

export type RequestMode = "mock" | "live";

export interface ExtensionSettings {
  endpointUrl: string;
}

export interface PipelineRefreshStatus {
  status: string;
  running: boolean;
  started: boolean;
  command: string[];
  configPath: string;
  logPath: string;
  pid: number | null;
  startedAt: string | null;
  finishedAt: string | null;
  exitCode: number | null;
  marketCount: number | null;
  artifactMarketCount: number | null;
  discoveredMarketCount: number | null;
  pairwiseMarketCount: number | null;
  progressStatus: string | null;
  progressMessage: string | null;
  reason?: string | null;
}

export interface PanelBootstrapData {
  settings: ExtensionSettings;
  fallbackContext: MarketClickContext;
  pipelineRefresh: PipelineRefreshStatus | null;
}

export interface PanelBootstrapRequestMessage {
  type: typeof PANEL_BOOTSTRAP_REQUEST;
}

export interface AttributeMoveRequestMessage {
  type: typeof ATTRIBUTE_MOVE_REQUEST;
  payload: {
    context: MarketClickContext;
    mode: RequestMode;
    endpointUrl?: string;
  };
}

export interface UpdateSettingsRequestMessage {
  type: typeof UPDATE_SETTINGS_REQUEST;
  payload: ExtensionSettings;
}

export interface AttributeMoveSynthesisRequestMessage {
  type: typeof ATTRIBUTE_MOVE_SYNTHESIS_REQUEST;
  payload: {
    context: MarketClickContext;
    endpointUrl?: string;
  };
}

export interface PipelineRefreshStatusRequestMessage {
  type: typeof PIPELINE_REFRESH_STATUS_REQUEST;
  payload?: {
    endpointUrl?: string;
  };
}

export interface PipelineRefreshTriggerRequestMessage {
  type: typeof PIPELINE_REFRESH_TRIGGER_REQUEST;
  payload?: {
    endpointUrl?: string;
  };
}

export interface PipelineRefreshStopRequestMessage {
  type: typeof PIPELINE_REFRESH_STOP_REQUEST;
  payload?: {
    endpointUrl?: string;
  };
}

export interface PanelBootstrapResponseMessage {
  ok: true;
  type: typeof PANEL_BOOTSTRAP_REQUEST;
  data: PanelBootstrapData;
}

export interface AttributeMoveResponseMessage {
  ok: true;
  type: typeof ATTRIBUTE_MOVE_REQUEST;
  data: AttributionResponse;
  meta: {
    mode: RequestMode;
    endpointUrl: string;
    mocked: boolean;
    fallbackReason?: string;
  };
}

export interface AttributeMoveSynthesisResponseMessage {
  ok: true;
  type: typeof ATTRIBUTE_MOVE_SYNTHESIS_REQUEST;
  data: AttributionSynthesisResponse;
  meta: {
    endpointUrl: string;
  };
}

export interface UpdateSettingsResponseMessage {
  ok: true;
  type: typeof UPDATE_SETTINGS_REQUEST;
  data: ExtensionSettings;
}

export interface PipelineRefreshStatusResponseMessage {
  ok: true;
  type: typeof PIPELINE_REFRESH_STATUS_REQUEST;
  data: PipelineRefreshStatus | null;
}

export interface PipelineRefreshTriggerResponseMessage {
  ok: true;
  type: typeof PIPELINE_REFRESH_TRIGGER_REQUEST;
  data: PipelineRefreshStatus | null;
}

export interface PipelineRefreshStopResponseMessage {
  ok: true;
  type: typeof PIPELINE_REFRESH_STOP_REQUEST;
  data: PipelineRefreshStatus | null;
}

export interface ErrorResponseMessage {
  ok: false;
  error: string;
  type?: RuntimeRequestMessage["type"];
}

export type RuntimeRequestMessage =
  | PanelBootstrapRequestMessage
  | AttributeMoveRequestMessage
  | AttributeMoveSynthesisRequestMessage
  | UpdateSettingsRequestMessage
  | PipelineRefreshStatusRequestMessage
  | PipelineRefreshTriggerRequestMessage
  | PipelineRefreshStopRequestMessage;

export type RuntimeResponseMessage =
  | PanelBootstrapResponseMessage
  | AttributeMoveResponseMessage
  | AttributeMoveSynthesisResponseMessage
  | UpdateSettingsResponseMessage
  | PipelineRefreshStatusResponseMessage
  | PipelineRefreshTriggerResponseMessage
  | PipelineRefreshStopResponseMessage
  | ErrorResponseMessage;
