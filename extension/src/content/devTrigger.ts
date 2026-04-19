import { mockMarketClickContext } from "../shared/fixtures/mockMarketClickContext";
import { extractMarketMetadata } from "./metadataExtractor";

const BUTTON_ID = "kalshify-dev-trigger";

export function installDevTrigger(onClick: (context: typeof mockMarketClickContext) => void): void {
  if (document.getElementById(BUTTON_ID)) {
    return;
  }

  const button = document.createElement("button");
  button.id = BUTTON_ID;
  button.type = "button";
  button.textContent = "Mock Move";
  button.style.position = "fixed";
  button.style.bottom = "18px";
  button.style.right = "384px";
  button.style.zIndex = "2147483647";
  button.style.padding = "10px 14px";
  button.style.borderRadius = "999px";
  button.style.border = "1px solid rgba(19, 33, 45, 0.14)";
  button.style.background = "#fffdf8";
  button.style.color = "#13212d";
  button.style.cursor = "pointer";
  button.style.font = '600 12px "IBM Plex Sans", sans-serif';

  button.addEventListener("click", () => {
    const metadata = extractMarketMetadata();
    onClick({
      ...mockMarketClickContext,
      marketId: metadata.marketId,
      marketTitle: metadata.marketTitle,
      marketQuestion: metadata.marketQuestion,
    });
  });

  document.body.appendChild(button);
}

