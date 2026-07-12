from __future__ import annotations

import socket
import stat
from pathlib import Path

import httpx
import pytest

from jasi.domain.processes import ProcessRecord
from jasi.runtime.errors import ToolRejected
from jasi.runtime.profile import (
    DRIFT_PROFILE,
    PASSIVE_PROFILE,
    PROACTIVE_PROFILE,
    SCHEDULED_PROFILE,
)
from jasi.tools.builtin import build_builtin_tool_registry
from jasi.tools.filesystem import FileWorkspace, create_filesystem_tools
from jasi.tools.messages import create_message_tools
from jasi.tools.processes import create_process_tools
from jasi.tools.registry import ToolExecutionContext, ToolRegistry
from jasi.tools.shell import CommandTaskManager, create_shell_tools
from jasi.tools.web import _render_response, _validate_public_url, create_web_tools
from tests.unit.fakes import FakeProcessLookup, FakeRepository

_BUILTIN_TOOL_NAMES = frozenset(
    {
        "get_current_time",
        "tool_search",
        "read_file",
        "list_dir",
        "write_file",
        "edit_file",
        "shell",
        "task_output",
        "task_stop",
        "web_search",
        "web_fetch",
        "search_messages",
        "fetch_messages",
        "list_processes",
    }
)


def _context(
    *,
    session_id: str = "telegram:1",
    conversation_id: int = 1,
) -> ToolExecutionContext:
    return ToolExecutionContext(
        work_id=1,
        session_id=session_id,
        conversation_id=conversation_id,
        profile="passive",
        timezone="Asia/Shanghai",
        allowed_tools=_BUILTIN_TOOL_NAMES,
        visible_tools=_BUILTIN_TOOL_NAMES,
    )


@pytest.mark.asyncio
async def test_filesystem_tools_are_atomic_and_confined_to_workspace(tmp_path: Path) -> None:
    workspace = FileWorkspace(tmp_path / "workspace")
    registry = ToolRegistry(create_filesystem_tools(workspace))

    written = await registry.execute(
        "write_file",
        {"path": "notes/example.txt", "content": "first\nsecond\n"},
        _context(),
    )
    assert written.content == {
        "path": "notes/example.txt",
        "created": True,
        "characters_written": 13,
    }

    target = workspace.root / "notes/example.txt"
    target.chmod(0o640)
    edited = await registry.execute(
        "edit_file",
        {"path": "notes/example.txt", "old_text": "second", "new_text": "updated"},
        _context(),
    )
    assert edited.content["replacements"] == 1
    assert stat.S_IMODE(target.stat().st_mode) == 0o640

    read = await registry.execute(
        "read_file",
        {"path": "notes/example.txt", "offset": 1, "limit": 1},
        _context(),
    )
    assert read.content["content"].endswith("updated")
    assert read.content["start_line"] == 2
    assert read.content["truncated"] is False

    listed = await registry.execute("list_dir", {"path": "notes"}, _context())
    assert listed.content["entries"] == [{"name": "example.txt", "type": "file", "size_bytes": 14}]

    outside = tmp_path / "outside.txt"
    outside.write_text("private", encoding="utf-8")
    (workspace.root / "outside-link.txt").symlink_to(outside)
    root_listing = await registry.execute("list_dir", {"path": "."}, _context())
    assert {entry["name"]: entry["type"] for entry in root_listing.content["entries"]} == {
        "notes": "directory",
        "outside-link.txt": "symlink",
    }
    for path in ("../outside.txt", "outside-link.txt"):
        with pytest.raises(ToolRejected, match="outside the tool workspace"):
            await registry.execute("read_file", {"path": path}, _context())


@pytest.mark.asyncio
async def test_shell_supports_full_bash_in_isolation_and_manages_owned_background_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = FileWorkspace(tmp_path / "workspace")
    manager = CommandTaskManager(workspace)
    if not manager.sandbox_available:
        pytest.skip(manager.sandbox_unavailable_reason or "sandbox unavailable")
    registry = ToolRegistry(create_shell_tools(workspace, manager))
    owner = _context()
    try:
        monkeypatch.setenv("JASI_TEST_SECRET", "must-not-leak")
        outside = tmp_path / "outside-secret.txt"
        outside.write_text("host-secret", encoding="utf-8")
        (workspace.root / "outside-link.txt").symlink_to(outside)
        foreground = await registry.execute(
            "shell",
            {
                "command": "printf 'b\\na\\n' | sort > result.txt && cat result.txt",
                "description": "run a pipeline",
            },
            owner,
        )
        assert foreground.content["exit_code"] == 0
        assert foreground.content["output"] == "a\nb\n"
        assert foreground.content["policy"]["risk"] == "write"
        assert (workspace.root / "result.txt").read_text(encoding="utf-8") == "a\nb\n"

        isolated = await registry.execute(
            "shell",
            {
                "command": (
                    "/bin/echo absolute-ok; "
                    "test ! -e /mnt/d/WorkSpace/myProject/agent/Jasi/.env; "
                    'test -z "$JASI_TEST_SECRET"; '
                    "printf isolated"
                ),
                "description": "verify isolation",
            },
            owner,
        )
        assert isolated.content["exit_code"] == 0
        assert isolated.content["output"] == "absolute-ok\nisolated"

        escaped = await registry.execute(
            "shell",
            {"command": "cat outside-link.txt", "description": "test host isolation"},
            owner,
        )
        assert escaped.content["exit_code"] != 0
        assert "host-secret" not in escaped.content["output"]

        network = await registry.execute(
            "shell",
            {
                "command": (
                    "python3 -c 'import socket; socket.create_connection((\"1.1.1.1\", 80), 1)'"
                ),
                "description": "test network isolation",
                "timeout": 5,
            },
            owner,
        )
        assert network.content["exit_code"] != 0

        rejected_commands = (
            "find . -exec echo {} +",
            "find . -delete",
            "git clean -fdx",
            "sudo echo unsafe",
            "bwrap --ro-bind / / true",
            '"$COMMAND" --version',
        )
        for command in rejected_commands:
            with pytest.raises(ToolRejected):
                await registry.execute(
                    "shell",
                    {"command": command, "description": "unsafe command"},
                    owner,
                )

        completed = await registry.execute(
            "shell",
            {
                "command": "echo background",
                "description": "run in background",
                "run_in_background": True,
            },
            owner,
        )
        assert completed.reveal_tools == ("task_output", "task_stop")
        completed_id = completed.content["background_task_id"]
        output = await registry.execute(
            "task_output",
            {"background_task_id": completed_id, "wait_seconds": 2},
            owner,
        )
        assert output.content["status"] == "completed"
        assert output.content["output"] == "background\n"

        running = await registry.execute(
            "shell",
            {
                "command": "sleep 30",
                "description": "wait in background",
                "run_in_background": True,
            },
            owner,
        )
        running_id = running.content["background_task_id"]
        running_output = await registry.execute(
            "task_output",
            {"background_task_id": running_id},
            owner,
        )
        assert running_output.content["status"] == "running"
        with pytest.raises(ToolRejected, match="was not found"):
            await registry.execute(
                "task_output",
                {"background_task_id": running_id},
                _context(session_id="telegram:2", conversation_id=2),
            )
        stopped = await registry.execute(
            "task_stop",
            {"background_task_id": running_id},
            owner,
        )
        assert stopped.content["status"] == "stopped"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_shell_rewrites_deletion_to_workspace_trash(tmp_path: Path) -> None:
    workspace = FileWorkspace(tmp_path / "workspace")
    manager = CommandTaskManager(workspace)
    if not manager.sandbox_available:
        pytest.skip(manager.sandbox_unavailable_reason or "sandbox unavailable")
    registry = ToolRegistry(create_shell_tools(workspace, manager))
    victim = workspace.root / "victim.txt"
    victim.write_text("recoverable", encoding="utf-8")
    try:
        outcome = await registry.execute(
            "shell",
            {"command": "rm -f victim.txt", "description": "remove a file"},
            _context(),
        )
    finally:
        await manager.aclose()

    assert outcome.content["exit_code"] == 0
    assert outcome.content["policy"]["risk"] == "destructive"
    assert outcome.content["policy"]["rewrites"][0]["kind"] == "soft_delete"
    assert not victim.exists()
    trashed = list((workspace.root / ".jasi-trash").rglob("victim.txt"))
    assert len(trashed) == 1
    assert trashed[0].read_text(encoding="utf-8") == "recoverable"


@pytest.mark.asyncio
async def test_shell_bounds_foreground_output_while_reading(tmp_path: Path) -> None:
    workspace = FileWorkspace(tmp_path / "workspace")
    manager = CommandTaskManager(workspace)
    if not manager.sandbox_available:
        pytest.skip(manager.sandbox_unavailable_reason or "sandbox unavailable")
    registry = ToolRegistry(create_shell_tools(workspace, manager), max_result_chars=100_000)
    try:
        outcome = await registry.execute(
            "shell",
            {
                "command": 'python3 -c \'print("a" * 40000); print("z" * 40000)\'',
                "description": "produce bounded output",
            },
            _context(),
        )
    finally:
        await manager.aclose()

    assert outcome.content["exit_code"] == 0
    assert outcome.content["truncated"] is True
    assert len(outcome.content["output"].encode()) < 31_000
    assert outcome.content["output"].startswith("a" * 100)
    assert outcome.content["output"].endswith("z" * 100 + "\n")


@pytest.mark.asyncio
async def test_web_tools_reject_private_targets_and_convert_html() -> None:
    registry = ToolRegistry(create_web_tools())

    for url in (
        "http://127.0.0.1/private",
        "http://user:secret@example.com/",
        "https://example.com:not-a-port/",
    ):
        with pytest.raises(ToolRejected):
            await registry.execute("web_fetch", {"url": url}, _context())

    with pytest.raises(ToolRejected, match="cannot be blank"):
        await registry.execute("web_search", {"query": "   "}, _context())

    request = httpx.Request("GET", "https://example.com/article")
    response = httpx.Response(
        200,
        headers={"content-type": "text/html; charset=utf-8"},
        request=request,
    )
    body = b"<html><body><h1>Title</h1><script>alert(1)</script><p>Body</p></body></html>"
    rendered = _render_response(response, body, "text", str(request.url))
    assert rendered["text"] == "Title\nBody"
    assert rendered["final_url"] == "https://example.com/article"


@pytest.mark.asyncio
async def test_web_fake_ip_dns_compatibility_never_allows_literal_reserved_ips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_getaddrinfo(*_args, **_kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("198.18.1.2", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    await _validate_public_url(
        "https://public.example/article",
        allow_fake_ip_dns=True,
    )
    with pytest.raises(ToolRejected):
        await _validate_public_url(
            "https://198.18.1.2/article",
            allow_fake_ip_dns=True,
        )


@pytest.mark.asyncio
async def test_message_tools_only_return_visible_current_conversation_messages() -> None:
    repository = FakeRepository()
    user = repository.add_message(role="user", content="needle from user", sequence=1)
    sent = repository.add_message(
        role="assistant",
        content="needle from assistant",
        sequence=2,
    )
    pending = repository.add_message(
        role="assistant",
        content="needle pending",
        sequence=3,
        delivery_status="pending",
    )
    system_error = repository.add_message(
        role="assistant",
        content="needle system error",
        sequence=4,
        origin="system_error",
    )
    other = repository.add_message(
        conversation_id=2,
        role="user",
        content="needle other conversation",
        sequence=1,
    )
    registry = ToolRegistry(create_message_tools(repository))

    searched = await registry.execute("search_messages", {"query": "needle"}, _context())
    assert [row["id"] for row in searched.content["messages"]] == [sent.id, user.id]
    assert searched.content["total"] == 2

    fetched = await registry.execute(
        "fetch_messages",
        {"message_ids": [sent.id, pending.id, system_error.id, other.id, user.id]},
        _context(),
    )
    assert [row["id"] for row in fetched.content["messages"]] == [sent.id, user.id]
    assert fetched.content["missing_ids"] == [pending.id, system_error.id, other.id]

    with pytest.raises(ToolRejected, match="cannot be blank"):
        await registry.execute("search_messages", {"query": "   "}, _context())


@pytest.mark.asyncio
async def test_process_tool_defaults_to_windows_and_filters_without_command_lines() -> None:
    lookup = FakeProcessLookup(
        {
            "runtime": (ProcessRecord(1, "python", 2.0, 10_000_000),),
            "windows": (
                ProcessRecord(10, "Code", 5.5, 500_000_000),
                ProcessRecord(11, "Code Helper", 1.0, 200_000_000),
                ProcessRecord(12, "Explorer", 3.0, 300_000_000),
            ),
        }
    )
    registry = ToolRegistry(create_process_tools(lookup))

    outcome = await registry.execute(
        "list_processes",
        {"scope": "auto", "name": "code", "sort_by": "memory"},
        _context(),
    )

    assert lookup.calls == ["windows"]
    assert outcome.content["scope"] == "windows"
    assert [row["pid"] for row in outcome.content["processes"]] == [10, 11]
    assert set(outcome.content["processes"][0]) == {
        "pid",
        "name",
        "cpu_seconds",
        "memory_mb",
    }


def test_builtin_builder_and_profiles_define_explicit_capability_boundaries(
    tmp_path: Path,
) -> None:
    repository = FakeRepository()
    workspace = FileWorkspace(tmp_path / "workspace")
    manager = CommandTaskManager(workspace)
    registry = build_builtin_tool_registry(
        workspace=workspace,
        messages=repository,
        processes=FakeProcessLookup(),
        command_tasks=manager,
    )

    assert registry.registered_names == _BUILTIN_TOOL_NAMES
    assert PASSIVE_PROFILE.allowed_tools == _BUILTIN_TOOL_NAMES
    assert SCHEDULED_PROFILE.allowed_tools == frozenset(
        {
            "get_current_time",
            "tool_search",
            "web_search",
            "web_fetch",
            "read_file",
            "list_dir",
            "search_messages",
            "fetch_messages",
        }
    )
    assert PROACTIVE_PROFILE.allowed_tools == frozenset(
        {"get_current_time", "tool_search", "web_search", "web_fetch"}
    )
    assert DRIFT_PROFILE.allowed_tools == frozenset(
        {
            "get_current_time",
            "tool_search",
            "web_search",
            "web_fetch",
            "search_messages",
            "fetch_messages",
        }
    )
    assert all(
        profile.base_tools == frozenset({"get_current_time", "tool_search"})
        for profile in (
            PASSIVE_PROFILE,
            SCHEDULED_PROFILE,
            PROACTIVE_PROFILE,
            DRIFT_PROFILE,
        )
    )
