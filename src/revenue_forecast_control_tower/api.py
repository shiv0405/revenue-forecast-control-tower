from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException

from .repository import FileBackedForecastRepository


def build_app(root: Path | None = None) -> FastAPI:
    project_root = root or Path(__file__).resolve().parents[2]
    repository = FileBackedForecastRepository(project_root)
    app = FastAPI(title="Revenue Forecast Control Tower", version="0.1.0")

    @app.get("/")
    def root() -> dict[str, object]:
        return {
            "service": "revenue-forecast-control-tower",
            "endpoints": [
                "/health",
                "/v1/forecast/summary",
                "/v1/forecast/regions",
                "/v1/pipeline/risk",
                "/v1/alerts",
                "/v1/scenarios/{scenario_name}",
            ],
        }

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/forecast/summary")
    def forecast_summary() -> dict[str, object]:
        return repository.summary()

    @app.get("/v1/forecast/regions")
    def forecast_regions() -> dict[str, object]:
        rows = repository.regional_forecast()
        return {"count": len(rows), "regions": rows}

    @app.get("/v1/pipeline/risk")
    def pipeline_risk(limit: int = 10) -> dict[str, object]:
        rows = repository.pipeline_risk()[: max(1, min(limit, 50))]
        return {"count": len(rows), "accounts": rows}

    @app.get("/v1/alerts")
    def alerts() -> dict[str, object]:
        rows = repository.alerts()
        return {"count": len(rows), "alerts": rows}

    @app.get("/v1/scenarios/{scenario_name}")
    def scenario(scenario_name: str) -> dict[str, object]:
        item = repository.scenario(scenario_name)
        if item is None:
            raise HTTPException(status_code=404, detail="scenario_not_found")
        return item

    return app
