from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jasi.runtime.errors import ToolRejected
from jasi.tools.registry import ToolSpec


def _get_current_time(arguments: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    raw_timezone = arguments.get("timezone") or context.get("timezone") or "Asia/Shanghai"
    if not isinstance(raw_timezone, str):
        raise ToolRejected("timezone must be a string")
    try:
        tz = ZoneInfo(raw_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ToolRejected(f"unknown timezone: {raw_timezone}") from exc
    now = datetime.now(tz)
    return {
        "timezone": raw_timezone,
        "iso8601": now.isoformat(timespec="seconds"),
        "unix_seconds": int(now.timestamp()),
    }


get_current_time_tool = ToolSpec(
    name="get_current_time",
    description="Return the current date and time for an IANA timezone.",
    parameters={
        "type": "object",
        "properties": {
            "timezone": {
                "type": "string",
                "description": "IANA timezone name, for example Asia/Shanghai or UTC.",
            }
        },
        "additionalProperties": False,
    },
    risk="low",
    handler=_get_current_time,
)
