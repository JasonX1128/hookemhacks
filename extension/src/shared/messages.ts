import type { AttributionResponse, MarketClickContext } from "./contracts";

export const PANEL_BOOTSTRAP_REQUEST = "market-move-explainer/panel-bootstrap";
export const ATTRIBUTE_MOVE_REQUEST = "market-move-explainer/attribute-move";
export const UPDATE_SETTINGS_REQUEST = "market-move-explainer/update-settings";

export type RequestMode = "mock" | "live";

export interface ExtensionSettings {
  endpointUrl: string;
}

export interface PanelBootstrapData {
  settings: ExtensionSettings;
  fallbackContext: MarketClickContext;
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

export interface UpdateSettingsResponseMessage {
  ok: true;
  type: typeof UPDATE_SETTINGS_REQUEST;
  data: ExtensionSettings;
}

export interface ErrorResponseMessage {
  ok: false;
  error: string;
  type?: RuntimeRequestMessage["type"];
}

export type RuntimeRequestMessage =
  | PanelBootstrapRequestMessage
  | AttributeMoveRequestMessage
  | UpdateSettingsRequestMessage;

export type RuntimeResponseMessage =
  | PanelBootstrapResponseMessage
  | AttributeMoveResponseMessage
  | UpdateSettingsResponseMessage
  | ErrorResponseMessage;
