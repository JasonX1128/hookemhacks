from __future__ import annotations

import uvicorn

from .core.config import get_settings


def main() -> None:
    settings = get_settings()
    module_target = f"{__package__}.main:app" if __package__ else "app.main:app"
    uvicorn.run(module_target, host=settings.host, port=settings.port, reload=False)


if __name__ == "__main__":
    main()
