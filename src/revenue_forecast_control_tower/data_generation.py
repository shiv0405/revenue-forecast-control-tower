from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from random import Random
from typing import Iterable

import pandas as pd


REGION_DIMENSION = [
    ("NA", "North America"),
    ("EU", "Europe"),
    ("AP", "Asia Pacific"),
    ("LA", "Latin America"),
    ("ME", "Middle East"),
    ("AF", "Africa"),
]
SEGMENTS = ["enterprise", "mid-market", "commercial"]
PRODUCT_FAMILIES = ["core-monitoring", "automation-suite", "security-analytics"]
CHANNELS = ["direct", "partner", "digital"]
STAGES = ["discover", "solution-fit", "proposal", "commit"]


@dataclass(frozen=True)
class GeneratedProjectData:
    accounts: pd.DataFrame
    bookings: pd.DataFrame
    pipeline: pd.DataFrame
    usage: pd.DataFrame


def _month_starts(months: int, end_month: date) -> list[date]:
    starts: list[date] = []
    cursor = date(end_month.year, end_month.month, 1)
    for _ in range(months):
        starts.append(cursor)
        if cursor.month == 1:
            cursor = date(cursor.year - 1, 12, 1)
        else:
            cursor = date(cursor.year, cursor.month - 1, 1)
    return list(reversed(starts))


def _quarter_start(value: date) -> date:
    month = ((value.month - 1) // 3) * 3 + 1
    return date(value.year, month, 1)


def generate_project_data(
    months: int = 18,
    accounts: int = 240,
    region_count: int = 6,
    seed: int = 23,
) -> GeneratedProjectData:
    rng = Random(seed)
    regions = REGION_DIMENSION[: max(2, min(region_count, len(REGION_DIMENSION)))]
    account_frame = _generate_accounts(accounts, regions, rng)
    month_starts = _month_starts(months, end_month=date(2026, 3, 1))
    bookings = _generate_bookings(account_frame, month_starts, rng)
    pipeline = _generate_pipeline(account_frame, month_starts, rng)
    usage = _generate_usage(account_frame, month_starts, rng)
    return GeneratedProjectData(accounts=account_frame, bookings=bookings, pipeline=pipeline, usage=usage)


def _generate_accounts(account_count: int, regions: list[tuple[str, str]], rng: Random) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    region_cycle = [region[0] for region in regions]
    for index in range(account_count):
        region_code = region_cycle[index % len(region_cycle)]
        segment = SEGMENTS[index % len(SEGMENTS)]
        product_family = PRODUCT_FAMILIES[index % len(PRODUCT_FAMILIES)]
        base_value = {
            "enterprise": rng.randint(180_000, 720_000),
            "mid-market": rng.randint(70_000, 240_000),
            "commercial": rng.randint(25_000, 90_000),
        }[segment]
        renewal_offset = rng.randint(20, 420)
        rows.append(
            {
                "account_id": f"ACC-{1000 + index}",
                "account_name": f"{region_code}-Portfolio-{index + 1:03d}",
                "region_code": region_code,
                "segment": segment,
                "product_family": product_family,
                "contract_value_usd": float(base_value),
                "renewal_date": date(2026, 4, 1) + timedelta(days=renewal_offset),
            }
        )
    return pd.DataFrame(rows)


def _generate_bookings(accounts: pd.DataFrame, month_starts: Iterable[date], rng: Random) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for month_start in month_starts:
        days = monthrange(month_start.year, month_start.month)[1]
        seasonality = 1.04 if month_start.month in {3, 6, 9, 12} else 0.88
        for _, account in accounts.iterrows():
            segment = str(account["segment"])
            contract_value = float(account["contract_value_usd"])
            monthly_target = contract_value * (0.025 if segment == "enterprise" else 0.038 if segment == "mid-market" else 0.045)
            for booking_index in range(3):
                booking_date = date(month_start.year, month_start.month, rng.randint(1, days))
                booking_amount = monthly_target * seasonality * rng.uniform(0.58, 1.05)
                rows.append(
                    {
                        "booking_date": booking_date,
                        "region_code": account["region_code"],
                        "account_id": account["account_id"],
                        "product_family": account["product_family"],
                        "booking_amount_usd": round(booking_amount, 2),
                        "target_amount_usd": round(monthly_target, 2),
                        "discount_pct": round(rng.uniform(0.02, 0.19), 3),
                        "channel": CHANNELS[(booking_index + rng.randint(0, 2)) % len(CHANNELS)],
                    }
                )
    return pd.DataFrame(rows).sort_values(["booking_date", "account_id"]).reset_index(drop=True)


def _generate_pipeline(accounts: pd.DataFrame, month_starts: Iterable[date], rng: Random) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for snapshot_month in month_starts:
        days = monthrange(snapshot_month.year, snapshot_month.month)[1]
        snapshot_date = date(snapshot_month.year, snapshot_month.month, min(days, 25))
        for _, account in accounts.iterrows():
            segment = str(account["segment"])
            opp_count = 4 if segment == "enterprise" else 3
            for opp_index in range(opp_count):
                probability = round(rng.uniform(0.22, 0.82), 3)
                stage = STAGES[min(int(probability * 4), 3)]
                weighted_pipeline = float(account["contract_value_usd"]) * rng.uniform(0.05, 0.24)
                slippage = "high" if probability < 0.35 or rng.random() < 0.18 else "medium" if probability < 0.58 else "low"
                expected_close = snapshot_date + timedelta(days=rng.randint(12, 120))
                rows.append(
                    {
                        "snapshot_date": snapshot_date,
                        "opportunity_id": f"OPP-{snapshot_month.year}{snapshot_month.month:02d}-{account['account_id']}-{opp_index}",
                        "account_id": account["account_id"],
                        "region_code": account["region_code"],
                        "stage_name": stage,
                        "weighted_pipeline_usd": round(weighted_pipeline, 2),
                        "commit_probability": probability,
                        "expected_close_date": expected_close,
                        "slippage_risk": slippage,
                    }
                )
    return pd.DataFrame(rows).sort_values(["snapshot_date", "opportunity_id"]).reset_index(drop=True)


def _generate_usage(accounts: pd.DataFrame, month_starts: Iterable[date], rng: Random) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for month_start in month_starts:
        for week_offset in (0, 7, 14, 21):
            usage_date = month_start + timedelta(days=week_offset)
            for _, account in accounts.iterrows():
                segment = str(account["segment"])
                base_users = {"enterprise": 620, "mid-market": 210, "commercial": 75}[segment]
                active_users = max(8, int(base_users * rng.uniform(0.62, 1.18)))
                weekly_sessions = active_users * rng.randint(4, 12)
                support_tickets = int(max(0, active_users * rng.uniform(0.01, 0.05)))
                satisfaction = round(rng.uniform(72, 96) - support_tickets * 0.12, 2)
                rows.append(
                    {
                        "usage_date": usage_date,
                        "account_id": account["account_id"],
                        "active_users": active_users,
                        "weekly_sessions": weekly_sessions,
                        "support_tickets": support_tickets,
                        "satisfaction_score": max(48.0, satisfaction),
                    }
                )
    return pd.DataFrame(rows).sort_values(["usage_date", "account_id"]).reset_index(drop=True)


def quarter_start_for_dataframe(value: pd.Timestamp) -> date:
    return _quarter_start(value.date())
