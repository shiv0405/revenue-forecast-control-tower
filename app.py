from __future__ import annotations

from pathlib import Path

from revenue_forecast_control_tower.api import build_app


app = build_app(Path(__file__).resolve().parent)
