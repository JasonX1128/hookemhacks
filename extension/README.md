# Chrome extension

This folder contains a Chrome MV3 extension that injects a floating market explainer panel on Kalshi pages.

## Local development

1. Run `npm install` at the repo root if dependencies are not installed yet.
2. Build once with `npm run build:extension`.
3. For rebuilds during development, run `npm run watch:extension`.
4. In Chrome, open `chrome://extensions`, enable Developer mode, and load `extension/dist` as an unpacked extension.

## Behavior

- The first render uses mocked attribution data so the panel is usable before any backend exists.
- The "POST to localhost" action sends the current market context to a configurable local endpoint.
- The default endpoint is `http://127.0.0.1:8000/attribute_move`, but the panel lets you change and save it in extension storage.

## Files

- `manifest.json`: MV3 manifest copied into `extension/dist/manifest.json` by the existing build script.
- `src/background/index.ts`: service worker, runtime message router, and dynamic content script registration.
- `src/content/index.ts`: floating panel injection, rendering, and message passing.
- `src/ui/panel.css`: isolated panel styles loaded inside a shadow root.
