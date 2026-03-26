from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from revenue_forecast_control_tower.api import build_app
from revenue_forecast_control_tower.config import ProjectPaths
from revenue_forecast_control_tower.service import run_project_generation


def test_api_serves_summary_and_alerts(tmp_path: Path) -> None:
    paths = ProjectPaths.from_root(tmp_path)
    run_project_generation(paths, months=6, account_count=48, region_count=3, seed=5)

    client = TestClient(build_app(tmp_path))

    root = client.get("/")
    summary = client.get("/v1/forecast/summary")
    regions = client.get("/v1/forecast/regions")
    alerts = client.get("/v1/alerts")
    scenario = client.get("/v1/scenarios/combined_play")

    assert root.status_code == 200
    assert root.json()["service"] == "revenue-forecast-control-tower"
    assert summary.status_code == 200
    assert summary.json()["accounts_in_scope"] == 48
    assert regions.status_code == 200
    assert regions.json()["count"] >= 1
    assert alerts.status_code == 200
    assert alerts.json()["count"] >= 1
    assert scenario.status_code == 200
    assert scenario.json()["revenue_uplift_usd"] > 0
