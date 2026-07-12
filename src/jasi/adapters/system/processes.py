from __future__ import annotations

import asyncio
import json
import os
import platform
import shutil
from pathlib import Path
from typing import Any

from jasi.domain.processes import (
    ProcessLookupUnavailable,
    ProcessRecord,
    ProcessScope,
    ProcessSnapshot,
)

_PROCESS_TIMEOUT_SECONDS = 15
_WINDOWS_SCRIPT = (
    "$OutputEncoding=[Console]::OutputEncoding=[Text.UTF8Encoding]::new();"
    "Get-Process | Select-Object Id,ProcessName,CPU,WorkingSet64 | ConvertTo-Json -Compress"
)


class LocalProcessInspector:
    def __init__(self) -> None:
        self._ps = shutil.which("ps") if os.name == "posix" else None
        self._powershell = _find_powershell()

    @property
    def available_scopes(self) -> frozenset[ProcessScope]:
        scopes: set[ProcessScope] = set()
        if self._ps:
            scopes.add("runtime")
        if self._powershell:
            scopes.add("windows")
        return frozenset(scopes)

    async def inspect(self, scope: ProcessScope) -> ProcessSnapshot:
        if scope not in self.available_scopes:
            raise ProcessLookupUnavailable(f"process scope is unavailable: {scope}")
        if scope == "windows":
            return ProcessSnapshot(scope=scope, processes=await self._windows_processes())
        return ProcessSnapshot(scope=scope, processes=await self._runtime_processes())

    async def _runtime_processes(self) -> tuple[ProcessRecord, ...]:
        assert self._ps is not None
        output = await _run(
            (
                self._ps,
                "-eo",
                "pid=,comm=,cputime=,rss=",
            ),
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
        rows: list[ProcessRecord] = []
        for line in output.splitlines():
            parts = line.strip().split(maxsplit=3)
            if len(parts) != 4:
                continue
            pid, name, cpu_time, rss_kib = parts
            try:
                rows.append(
                    ProcessRecord(
                        pid=int(pid),
                        name=name,
                        cpu_seconds=_parse_cpu_time(cpu_time),
                        memory_bytes=int(rss_kib) * 1024,
                    )
                )
            except ValueError:
                continue
        return tuple(rows)

    async def _windows_processes(self) -> tuple[ProcessRecord, ...]:
        assert self._powershell is not None
        output = await _run(
            (
                self._powershell,
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                _WINDOWS_SCRIPT,
            ),
            env=_windows_interop_environment(),
        )
        try:
            payload: Any = json.loads(output.lstrip("\ufeff") or "[]")
        except ValueError as exc:
            raise ProcessLookupUnavailable("Windows process output was invalid") from exc
        items = payload if isinstance(payload, list) else [payload]
        rows: list[ProcessRecord] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            try:
                pid = int(item["Id"])
            except (KeyError, TypeError, ValueError):
                continue
            rows.append(
                ProcessRecord(
                    pid=pid,
                    name=str(item.get("ProcessName") or "unknown"),
                    cpu_seconds=_optional_float(item.get("CPU")),
                    memory_bytes=_optional_int(item.get("WorkingSet64")),
                )
            )
        return tuple(rows)


async def _run(argv: tuple[str, ...], *, env: dict[str, str]) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            *argv,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=_PROCESS_TIMEOUT_SECONDS,
        )
    except TimeoutError as exc:
        if "process" in locals() and process.returncode is None:
            process.kill()
            await process.communicate()
        raise ProcessLookupUnavailable("process inspection timed out") from exc
    except asyncio.CancelledError:
        if "process" in locals() and process.returncode is None:
            process.kill()
            await process.communicate()
        raise
    except OSError as exc:
        raise ProcessLookupUnavailable("process inspector could not be started") from exc
    if process.returncode != 0:
        error = stderr.decode("utf-8", errors="replace").strip()
        raise ProcessLookupUnavailable(error[:300] or "process inspection failed")
    return stdout.decode("utf-8", errors="replace").strip()


def _find_powershell() -> str | None:
    discovered = shutil.which("powershell.exe") or shutil.which("powershell")
    if discovered:
        return discovered
    if platform.system() == "Linux":
        candidate = Path("/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe")
        if candidate.is_file():
            return str(candidate)
    return None


def _windows_interop_environment() -> dict[str, str]:
    allowed = {
        "LANG",
        "LC_ALL",
        "PATH",
        "SystemRoot",
        "WINDIR",
        "WSLENV",
        "WSL_INTEROP",
    }
    return {name: value for name, value in os.environ.items() if name in allowed}


def _parse_cpu_time(value: str) -> float:
    days = 0
    clock = value
    if "-" in value:
        raw_days, clock = value.split("-", 1)
        days = int(raw_days)
    parts = [int(part) for part in clock.split(":")]
    if len(parts) == 2:
        hours = 0
        minutes, seconds = parts
    elif len(parts) == 3:
        hours, minutes, seconds = parts
    else:
        raise ValueError("invalid CPU time")
    return float(days * 86400 + hours * 3600 + minutes * 60 + seconds)


def _optional_float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
