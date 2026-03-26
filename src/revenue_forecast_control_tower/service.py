from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from .config import ProjectPaths
from .data_generation import GeneratedProjectData, generate_project_data, quarter_start_for_dataframe
from .models import ForecastSummary, ScenarioResult

PIPELINE_CONVERSION_FACTOR = 0.08


def run_project_generation(
    paths: ProjectPaths,
    months: int = 18,
    account_count: int = 240,
    region_count: int = 6,
    seed: int = 23,
) -> dict[str, object]:
    paths.ensure()
    generated = generate_project_data(months=months, accounts=account_count, region_count=region_count, seed=seed)
    _write_raw_data(paths, generated)
    outputs = build_reporting_outputs(paths, generated)
    _write_dashboard_bundle(paths, outputs["dashboard_snapshot"])
    return outputs


def build_reporting_outputs(paths: ProjectPaths, generated: GeneratedProjectData) -> dict[str, object]:
    bookings = generated.bookings.copy()
    bookings["booking_date"] = pd.to_datetime(bookings["booking_date"])
    bookings["quarter_start"] = bookings["booking_date"].apply(quarter_start_for_dataframe)

    pipeline = generated.pipeline.copy()
    pipeline["snapshot_date"] = pd.to_datetime(pipeline["snapshot_date"])
    latest_snapshot = pipeline["snapshot_date"].max()
    latest_pipeline = pipeline[pipeline["snapshot_date"] == latest_snapshot].copy()

    usage = generated.usage.copy()
    usage["usage_date"] = pd.to_datetime(usage["usage_date"])
    latest_usage = usage[usage["usage_date"] == usage["usage_date"].max()].copy()

    current_quarter = bookings["quarter_start"].max()
    quarter_bookings = bookings[bookings["quarter_start"] == current_quarter]

    forecast_summary = _build_forecast_summary(generated.accounts, quarter_bookings, latest_pipeline, latest_usage)
    regional_forecast = _build_regional_forecast(quarter_bookings, latest_pipeline)
    pipeline_risk = _build_pipeline_risk(generated.accounts, latest_pipeline, latest_usage)
    alerts = _build_alerts(pipeline_risk, forecast_summary.forecast_gap_usd)
    scenarios = _build_scenarios(forecast_summary)

    dashboard_snapshot = {
        "title": "Revenue Forecast Control Tower",
        "summary": asdict(forecast_summary),
        "regions": _json_ready(regional_forecast.head(8).to_dict(orient="records")),
        "pipeline_risk": _json_ready(pipeline_risk.head(12).to_dict(orient="records")),
        "alerts": _json_ready(alerts),
        "scenarios": _json_ready([asdict(item) for item in scenarios]),
    }

    _write_outputs(paths, forecast_summary, regional_forecast, pipeline_risk, dashboard_snapshot)
    return {
        "summary": forecast_summary,
        "regional_forecast": regional_forecast,
        "pipeline_risk": pipeline_risk,
        "dashboard_snapshot": dashboard_snapshot,
        "alerts": alerts,
        "scenarios": scenarios,
    }


def _build_forecast_summary(
    accounts: pd.DataFrame,
    quarter_bookings: pd.DataFrame,
    latest_pipeline: pd.DataFrame,
    latest_usage: pd.DataFrame,
) -> ForecastSummary:
    current_bookings = float(quarter_bookings["booking_amount_usd"].sum())
    target = float(quarter_bookings["target_amount_usd"].sum())
    weighted_pipeline = float(latest_pipeline["weighted_pipeline_usd"].sum())
    usage_join = latest_usage.merge(accounts[["account_id", "contract_value_usd", "renewal_date"]], on="account_id", how="left")
    usage_join["renewal_date"] = pd.to_datetime(usage_join["renewal_date"])
    renewal_risk = usage_join[
        (usage_join["satisfaction_score"] < 76)
        | (usage_join["support_tickets"] >= 12)
        | (usage_join["renewal_date"] <= usage_join["usage_date"] + pd.Timedelta(days=120))
    ]["contract_value_usd"].sum()
    projection = current_bookings + weighted_pipeline * PIPELINE_CONVERSION_FACTOR
    forecast_gap = max(target - projection, 0.0)
    return ForecastSummary(
        accounts_in_scope=int(accounts["account_id"].nunique()),
        current_quarter_bookings_usd=round(current_bookings, 2),
        quarterly_target_usd=round(target, 2),
        attainment_pct=round((current_bookings / target) * 100 if target else 0.0, 1),
        weighted_pipeline_usd=round(weighted_pipeline, 2),
        projected_renewal_risk_usd=round(float(renewal_risk), 2),
        forecast_gap_usd=round(float(forecast_gap), 2),
    )


def _build_regional_forecast(quarter_bookings: pd.DataFrame, latest_pipeline: pd.DataFrame) -> pd.DataFrame:
    bookings_group = (
        quarter_bookings.groupby("region_code", as_index=False)
        .agg(bookings_usd=("booking_amount_usd", "sum"), target_usd=("target_amount_usd", "sum"))
    )
    pipeline_group = latest_pipeline.groupby("region_code", as_index=False).agg(weighted_pipeline_usd=("weighted_pipeline_usd", "sum"))
    merged = bookings_group.merge(pipeline_group, on="region_code", how="left").fillna({"weighted_pipeline_usd": 0.0})
    merged["attainment_pct"] = (merged["bookings_usd"] / merged["target_usd"] * 100).round(1)
    merged["projected_attainment_pct"] = ((merged["bookings_usd"] + merged["weighted_pipeline_usd"] * PIPELINE_CONVERSION_FACTOR) / merged["target_usd"] * 100).round(1)
    merged["forecast_gap_usd"] = (merged["target_usd"] - (merged["bookings_usd"] + merged["weighted_pipeline_usd"] * PIPELINE_CONVERSION_FACTOR)).clip(lower=0).round(2)
    return merged.sort_values(["forecast_gap_usd", "weighted_pipeline_usd"], ascending=[False, False]).reset_index(drop=True)


def _build_pipeline_risk(accounts: pd.DataFrame, latest_pipeline: pd.DataFrame, latest_usage: pd.DataFrame) -> pd.DataFrame:
    per_account = (
        latest_pipeline.groupby(["account_id", "region_code"], as_index=False)
        .agg(
            weighted_pipeline_usd=("weighted_pipeline_usd", "sum"),
            avg_commit_probability=("commit_probability", "mean"),
            high_risk_flags=("slippage_risk", lambda values: sum(item == "high" for item in values)),
        )
    )
    usage_group = latest_usage.groupby("account_id", as_index=False).agg(
        active_users=("active_users", "mean"),
        support_tickets=("support_tickets", "mean"),
        satisfaction_score=("satisfaction_score", "mean"),
    )
    merged = per_account.merge(accounts, on=["account_id", "region_code"], how="left").merge(usage_group, on="account_id", how="left")
    merged["risk_score"] = (
        (1 - merged["avg_commit_probability"]) * 40
        + merged["high_risk_flags"] * 12
        + (merged["support_tickets"] / merged["support_tickets"].max()) * 18
        + ((100 - merged["satisfaction_score"]) / 100) * 30
    ).round(1)
    merged["recommended_action"] = merged.apply(_recommended_action, axis=1)
    return merged.sort_values(["risk_score", "weighted_pipeline_usd"], ascending=[False, False]).reset_index(drop=True)


def _recommended_action(row: pd.Series) -> str:
    if row["risk_score"] >= 62:
        return "Escalate commercial review and tighten close plan"
    if row["risk_score"] >= 44:
        return "Run renewal recovery plan and inspect stage slippage"
    return "Monitor account momentum and update scenario assumptions"


def _build_alerts(pipeline_risk: pd.DataFrame, forecast_gap_usd: float) -> list[dict[str, object]]:
    alerts: list[dict[str, object]] = []
    for _, row in pipeline_risk.head(5).iterrows():
        alerts.append(
            {
                "severity": "high" if row["risk_score"] >= 62 else "medium",
                "account_id": row["account_id"],
                "region_code": row["region_code"],
                "message": f"{row['account_name']} is carrying elevated forecast risk with score {row['risk_score']}.",
                "recommended_action": row["recommended_action"],
            }
        )
    if forecast_gap_usd > 0:
        alerts.insert(
            0,
            {
                "severity": "high" if forecast_gap_usd > 400000 else "medium",
                "account_id": "portfolio",
                "region_code": "all",
                "message": f"Current-quarter forecast gap remains ${forecast_gap_usd:,.0f}.",
                "recommended_action": "Review conversion assumptions, renewal slippage, and regional recovery plans.",
            },
        )
    return alerts


def _build_scenarios(summary: ForecastSummary) -> list[ScenarioResult]:
    base_projection = summary.current_quarter_bookings_usd + summary.weighted_pipeline_usd * PIPELINE_CONVERSION_FACTOR
    conversion_lift = base_projection + summary.weighted_pipeline_usd * 0.03
    renewal_recovery = base_projection + summary.projected_renewal_risk_usd * 0.08
    combined = base_projection + summary.weighted_pipeline_usd * 0.03 + summary.projected_renewal_risk_usd * 0.08
    return [
        ScenarioResult(
            scenario_name="baseline_plus_conversion",
            revenue_uplift_usd=round(conversion_lift - base_projection, 2),
            attainment_pct=round(conversion_lift / summary.quarterly_target_usd * 100, 1),
            notes=["Assumes a modest lift in conversion quality across late-stage pipeline."],
        ),
        ScenarioResult(
            scenario_name="renewal_recovery",
            revenue_uplift_usd=round(renewal_recovery - base_projection, 2),
            attainment_pct=round(renewal_recovery / summary.quarterly_target_usd * 100, 1),
            notes=["Assumes partial recovery of at-risk renewals through intervention plans."],
        ),
        ScenarioResult(
            scenario_name="combined_play",
            revenue_uplift_usd=round(combined - base_projection, 2),
            attainment_pct=round(combined / summary.quarterly_target_usd * 100, 1),
            notes=["Combines improved conversion execution with renewal recovery activity."],
        ),
    ]


def _write_raw_data(paths: ProjectPaths, generated: GeneratedProjectData) -> None:
    generated.accounts.to_csv(paths.raw_dir / "customer_contracts.csv", index=False)
    generated.bookings.to_csv(paths.raw_dir / "daily_bookings.csv", index=False)
    generated.pipeline.to_csv(paths.raw_dir / "pipeline_snapshots.csv", index=False)
    generated.usage.to_csv(paths.raw_dir / "product_usage_signals.csv", index=False)


def _write_outputs(
    paths: ProjectPaths,
    summary: ForecastSummary,
    regional_forecast: pd.DataFrame,
    pipeline_risk: pd.DataFrame,
    dashboard_snapshot: dict[str, object],
) -> None:
    (paths.processed_dir / "forecast_summary.json").write_text(json.dumps(_json_ready(asdict(summary)), indent=2), encoding="utf-8")
    regional_forecast.to_csv(paths.processed_dir / "regional_forecast.csv", index=False)
    pipeline_risk.to_csv(paths.processed_dir / "pipeline_risk.csv", index=False)
    (paths.processed_dir / "dashboard_snapshot.json").write_text(json.dumps(_json_ready(dashboard_snapshot), indent=2), encoding="utf-8")
    (paths.artifacts_dir / "kpi_brief.md").write_text(_kpi_brief(summary, regional_forecast), encoding="utf-8")
    (paths.artifacts_dir / "executive_summary.html").write_text(_executive_html(summary, regional_forecast, pipeline_risk), encoding="utf-8")


def _write_dashboard_bundle(paths: ProjectPaths, dashboard_snapshot: dict[str, object]) -> None:
    data_dir = paths.dashboard_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "dashboard_snapshot.json").write_text(json.dumps(_json_ready(dashboard_snapshot), indent=2), encoding="utf-8")


def _json_ready(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    return value


def _kpi_brief(summary: ForecastSummary, regional_forecast: pd.DataFrame) -> str:
    worst_region = regional_forecast.iloc[0]
    return "\n".join(
        [
            "# KPI Brief",
            "",
            f"- Accounts in scope: {summary.accounts_in_scope}",
            f"- Current-quarter bookings: ${summary.current_quarter_bookings_usd:,.0f}",
            f"- Quarterly target: ${summary.quarterly_target_usd:,.0f}",
            f"- Attainment: {summary.attainment_pct:.1f}%",
            f"- Weighted pipeline: ${summary.weighted_pipeline_usd:,.0f}",
            f"- Forecast gap: ${summary.forecast_gap_usd:,.0f}",
            f"- Highest gap region: {worst_region['region_code']} at ${worst_region['forecast_gap_usd']:,.0f}",
        ]
    ) + "\n"


def _executive_html(summary: ForecastSummary, regional_forecast: pd.DataFrame, pipeline_risk: pd.DataFrame) -> str:
    top_region = regional_forecast.iloc[0]
    top_account = pipeline_risk.iloc[0]
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Revenue Forecast Control Tower</title>
  <style>
    body {{ font-family: "Segoe UI", sans-serif; margin: 0; background: #f4f7fb; color: #1b2530; }}
    .wrap {{ max-width: 1100px; margin: 0 auto; padding: 32px; }}
    .hero {{ background: linear-gradient(135deg, #0f3057, #1f6f8b); color: white; padding: 28px; border-radius: 22px; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-top: 24px; }}
    .card {{ background: white; border-radius: 18px; padding: 18px; box-shadow: 0 12px 36px rgba(23, 39, 57, 0.08); }}
    h1, h2, h3 {{ margin-top: 0; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid #d9e1ea; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Revenue Forecast Control Tower</h1>
      <p>Integrated commercial planning view for bookings attainment, pipeline quality, renewal exposure, and account-level forecast risk.</p>
    </section>
    <section class="grid">
      <div class="card"><h3>Bookings</h3><strong>${summary.current_quarter_bookings_usd:,.0f}</strong></div>
      <div class="card"><h3>Attainment</h3><strong>{summary.attainment_pct:.1f}%</strong></div>
      <div class="card"><h3>Forecast Gap</h3><strong>${summary.forecast_gap_usd:,.0f}</strong></div>
    </section>
    <section class="card" style="margin-top: 24px;">
      <h2>Regional Pressure</h2>
      <p>{top_region['region_code']} is showing the largest forecast gap at ${top_region['forecast_gap_usd']:,.0f} while carrying ${top_region['weighted_pipeline_usd']:,.0f} of weighted pipeline.</p>
      <table>
        <thead><tr><th>Region</th><th>Bookings</th><th>Target</th><th>Projected Attainment</th></tr></thead>
        <tbody>
          {''.join(f"<tr><td>{row.region_code}</td><td>${row.bookings_usd:,.0f}</td><td>${row.target_usd:,.0f}</td><td>{row.projected_attainment_pct:.1f}%</td></tr>" for row in regional_forecast.head(5).itertuples())}
        </tbody>
      </table>
    </section>
    <section class="card" style="margin-top: 24px;">
      <h2>Account To Watch</h2>
      <p>{top_account['account_name']} carries the highest composite risk score at {top_account['risk_score']:.1f}. Recommended action: {top_account['recommended_action']}.</p>
    </section>
  </div>
</body>
</html>
"""
