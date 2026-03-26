from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ForecastSummary:
    accounts_in_scope: int
    current_quarter_bookings_usd: float
    quarterly_target_usd: float
    attainment_pct: float
    weighted_pipeline_usd: float
    projected_renewal_risk_usd: float
    forecast_gap_usd: float


@dataclass(frozen=True)
class ScenarioResult:
    scenario_name: str
    revenue_uplift_usd: float
    attainment_pct: float
    notes: list[str]
