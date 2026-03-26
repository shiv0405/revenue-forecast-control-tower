from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import ProjectPaths


class FileBackedForecastRepository:
    def __init__(self, root: Path) -> None:
        self.paths = ProjectPaths.from_root(root)

    def dashboard_snapshot(self) -> dict[str, Any]:
        path = self.paths.processed_dir / "dashboard_snapshot.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def summary(self) -> dict[str, Any]:
        return self.dashboard_snapshot()["summary"]

    def regional_forecast(self) -> list[dict[str, Any]]:
        return self.dashboard_snapshot()["regions"]

    def pipeline_risk(self) -> list[dict[str, Any]]:
        return self.dashboard_snapshot()["pipeline_risk"]

    def alerts(self) -> list[dict[str, Any]]:
        return self.dashboard_snapshot()["alerts"]

    def scenario(self, scenario_name: str) -> dict[str, Any] | None:
        for item in self.dashboard_snapshot()["scenarios"]:
            if item["scenario_name"] == scenario_name:
                return item
        return None


def load_pipeline_csv(root: Path) -> pd.DataFrame:
    return pd.read_csv(ProjectPaths.from_root(root).processed_dir / "pipeline_risk.csv")
