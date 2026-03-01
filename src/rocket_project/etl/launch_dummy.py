from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

def make_dummy_payload() -> dict[str, Any]:
    return {
        "results": [
            {"id": "A1", "name": "Demo Launch", "net": "2026-02-24T00:00:00Z"},
            {"id": "A2", "name": "Another Launch", "net": "2026-02-25T00:00:00Z"},
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

def transform_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] =[]
    for item in payload.get("results", []):
        rows.append(
            {
                "launch_id": item.get("id"),
                "name": item.get("name"),
                "net": item.get("net"),
            }
        )
    return rows
