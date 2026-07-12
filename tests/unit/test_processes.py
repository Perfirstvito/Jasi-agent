from __future__ import annotations

import json

import pytest

from jasi.adapters.system import processes as process_adapter
from jasi.adapters.system.processes import LocalProcessInspector


@pytest.mark.asyncio
async def test_local_process_inspector_parses_runtime_processes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run(_argv, *, env):
        assert "JASI_OPENAI_API_KEY" not in env
        return "1 init 00:00:03 1024\n42 python 1-01:02:03 2048"

    monkeypatch.setattr(process_adapter, "_run", fake_run)
    monkeypatch.setattr(process_adapter, "_find_powershell", lambda: None)
    inspector = LocalProcessInspector()

    snapshot = await inspector.inspect("runtime")

    assert [(row.pid, row.name) for row in snapshot.processes] == [(1, "init"), (42, "python")]
    assert snapshot.processes[0].cpu_seconds == 3
    assert snapshot.processes[1].cpu_seconds == 90_123
    assert snapshot.processes[1].memory_bytes == 2 * 1024 * 1024


@pytest.mark.asyncio
async def test_local_process_inspector_parses_windows_json_without_command_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = [
        {"Id": 7, "ProcessName": "Code", "CPU": 12.5, "WorkingSet64": 104857600},
    ]

    async def fake_run(argv, *, env):
        assert argv[0] == "powershell.exe"
        assert "JASI_OPENAI_API_KEY" not in env
        return json.dumps(payload)

    monkeypatch.setattr(process_adapter, "_run", fake_run)
    monkeypatch.setattr(process_adapter, "_find_powershell", lambda: "powershell.exe")
    inspector = LocalProcessInspector()

    snapshot = await inspector.inspect("windows")

    assert snapshot.processes[0].pid == 7
    assert snapshot.processes[0].name == "Code"
    assert snapshot.processes[0].memory_bytes == 104857600
