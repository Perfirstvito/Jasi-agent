from __future__ import annotations

from typing import Any

from jasi.domain.processes import ProcessLookupUnavailable, ProcessRecord, ProcessScope
from jasi.ports.tools import ProcessLookupPort
from jasi.runtime.errors import ToolRejected
from jasi.tools.registry import ToolExecutionContext, ToolSpec

_MAX_RESULTS = 50


def create_process_tools(processes: ProcessLookupPort) -> list[ToolSpec]:
    async def list_processes(
        arguments: dict[str, Any], _context: ToolExecutionContext
    ) -> dict[str, Any]:
        requested_scope = arguments.get("scope", "auto")
        scope = _resolve_scope(requested_scope, processes.available_scopes)
        try:
            snapshot = await processes.inspect(scope)
        except ProcessLookupUnavailable as exc:
            raise ToolRejected(str(exc)) from exc
        name_filter = str(arguments.get("name", "")).strip().casefold()
        rows = [
            row
            for row in snapshot.processes
            if not name_filter or name_filter in row.name.casefold()
        ]
        sort_by = arguments.get("sort_by", "memory")
        rows.sort(key=lambda row: _sort_key(row, sort_by))
        limit = int(arguments.get("limit", 20))
        selected = rows[:limit]
        return {
            "scope": snapshot.scope,
            "count": len(selected),
            "total": len(rows),
            "processes": [_process_result(row) for row in selected],
            "note": (
                "windows is the user's Windows host; runtime is the Jasi Linux/WSL environment."
            ),
        }

    return [
        ToolSpec(
            name="list_processes",
            description=(
                "List running processes without exposing command-line arguments or environment "
                "variables. Use scope='windows' for the user's Windows computer and "
                "scope='runtime' for Jasi's Linux/WSL environment."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "string",
                        "enum": ["auto", "runtime", "windows"],
                        "default": "auto",
                    },
                    "name": {"type": "string", "maxLength": 200},
                    "sort_by": {
                        "type": "string",
                        "enum": ["memory", "cpu", "name", "pid"],
                        "default": "memory",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": _MAX_RESULTS,
                        "default": 20,
                    },
                },
                "additionalProperties": False,
            },
            risk="read-only",
            handler=list_processes,
            search_terms=(
                "list processes",
                "running programs",
                "process manager",
                "进程",
                "电脑进程",
                "运行程序",
                "任务管理器",
            ),
        )
    ]


def _resolve_scope(requested: str, available: frozenset[ProcessScope]) -> ProcessScope:
    if requested == "auto":
        if "windows" in available:
            return "windows"
        if "runtime" in available:
            return "runtime"
        raise ToolRejected("process inspection is unavailable")
    scope: ProcessScope = requested
    if scope not in available:
        raise ToolRejected(f"process scope is unavailable: {scope}")
    return scope


def _sort_key(row: ProcessRecord, sort_by: str):
    if sort_by == "name":
        return (row.name.casefold(), row.pid)
    if sort_by == "pid":
        return (row.pid,)
    if sort_by == "cpu":
        return (-(row.cpu_seconds or 0), row.name.casefold(), row.pid)
    return (-(row.memory_bytes or 0), row.name.casefold(), row.pid)


def _process_result(row: ProcessRecord) -> dict[str, Any]:
    return {
        "pid": row.pid,
        "name": row.name,
        "cpu_seconds": round(row.cpu_seconds, 3) if row.cpu_seconds is not None else None,
        "memory_mb": round(row.memory_bytes / 1024 / 1024, 1)
        if row.memory_bytes is not None
        else None,
    }
