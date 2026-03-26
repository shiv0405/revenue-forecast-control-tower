from __future__ import annotations

from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    alerts = event.get("alerts", [])
    outbound: list[dict[str, str]] = []
    for alert in alerts:
        severity = str(alert.get("severity", "medium")).upper()
        account_id = str(alert.get("account_id", "unknown"))
        message = str(alert.get("message", ""))
        outbound.append(
            {
                "channel": "commercial-forecast-alerts",
                "subject": f"[{severity}] Forecast signal for {account_id}",
                "body": message,
            }
        )
    return {"notifications": outbound, "count": len(outbound)}
