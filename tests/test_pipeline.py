from __future__ import annotations

import json
from pathlib import Path

from revenue_forecast_control_tower.config import ProjectPaths
from revenue_forecast_control_tower.service import run_project_generation


def test_run_project_generation_writes_outputs(tmp_path: Path) -> None:
    paths = ProjectPaths.from_root(tmp_path)
    outputs = run_project_generation(paths, months=8, account_count=72, region_count=4, seed=7)

    assert outputs["summary"].accounts_in_scope == 72
    assert (paths.raw_dir / "daily_bookings.csv").exists()
    assert (paths.processed_dir / "forecast_summary.json").exists()
    assert (paths.artifacts_dir / "executive_summary.html").exists()
    dashboard_snapshot = json.loads((paths.processed_dir / "dashboard_snapshot.json").read_text(encoding="utf-8"))
    assert dashboard_snapshot["alerts"]
    assert dashboard_snapshot["regions"]
