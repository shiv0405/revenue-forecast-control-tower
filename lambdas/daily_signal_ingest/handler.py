from __future__ import annotations

from datetime import datetime
from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    records = event.get("records", [])
    normalized: list[dict[str, Any]] = []
    for record in records:
        event_type = str(record.get("event_type", "")).strip()
        account_id = str(record.get("account_id", "")).strip()
        event_date = str(record.get("event_date", "")).strip()
        value = float(record.get("value", 0.0))
        if event_type not in {"booking", "usage"} or not account_id or not event_date:
            continue
        normalized.append(
            {
                "target_table": "fact_daily_bookings" if event_type == "booking" else "fact_product_usage",
                "account_id": account_id,
                "event_date": event_date,
                "value": round(value, 2),
                "ingested_at": datetime.utcnow().isoformat() + "Z",
            }
        )
    return {"accepted": len(normalized), "records": normalized}
