from __future__ import annotations

import asyncio
import os
import shlex
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from jasi.runtime.errors import ToolRejected
from jasi.tools.filesystem import FileWorkspace
from jasi.tools.registry import ToolExecutionContext, ToolOutcome, ToolSpec

_ALLOWED_COMMANDS = frozenset(
    {
        "cat",
        "date",
        "echo",
        "find",
        "grep",
        "head",
        "ls",
        "pwd",
        "rg",
        "sleep",
        "sort",
        "tail",
        "uniq",
        "wc",
    }
)
_SHELL_OPERATORS = frozenset({"|", "||", "&", "&&", ";", ">", ">>", "<", "<<"})
_MAX_OUTPUT_BYTES = 30_000
_MAX_BACKGROUND_TASKS = 32
_DEFAULT_TIMEOUT_SECONDS = 30
_MAX_TIMEOUT_SECONDS = 120
_COMMAND_PATH = "/usr/local/bin:/usr/bin:/bin"


@dataclass
class _CommandTask:
    task_id: str
    owner_session_id: str
    process: asyncio.subprocess.Process
    command: str
    description: str
    started_at: float
    output: bytearray
    pump_task: asyncio.Task[None] | None = None
    timeout_task: asyncio.Task[None] | None = None
    finish_reason: str = "running"
    output_truncated: bool = False
    finished_at: float | None = None


class CommandTaskManager:
    def __init__(self, workspace: FileWorkspace) -> None:
        self._workspace = workspace
        self._tasks: dict[str, _CommandTask] = {}

    async def run_foreground(
        self,
        *,
        argv: list[str],
        cwd: Path,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        process = await self._spawn(argv, cwd)
        started = time.monotonic()
        timed_out = False
        try:
            stdout, _ = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            timed_out = True
            _kill_process_tree(process)
            stdout, _ = await process.communicate()
        except asyncio.CancelledError:
            _kill_process_tree(process)
            await process.communicate()
            raise
        output, truncated = _bounded_output(stdout)
        return {
            "command": shlex.join(argv),
            "exit_code": process.returncode,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "timed_out": timed_out,
            "output": output,
            "truncated": truncated,
        }

    async def start_background(
        self,
        *,
        argv: list[str],
        cwd: Path,
        owner_session_id: str,
        description: str,
        timeout_seconds: int | None,
    ) -> dict[str, Any]:
        self._prune()
        if len(self._tasks) >= _MAX_BACKGROUND_TASKS:
            raise ToolRejected("too many background command tasks")
        process = await self._spawn(argv, cwd)
        task_id = uuid4().hex[:12]
        record = _CommandTask(
            task_id=task_id,
            owner_session_id=owner_session_id,
            process=process,
            command=shlex.join(argv),
            description=description,
            started_at=time.monotonic(),
            output=bytearray(),
        )
        self._tasks[task_id] = record
        record.pump_task = asyncio.create_task(
            self._pump(record),
            name=f"jasi-command-{task_id}",
        )
        if timeout_seconds is not None:
            record.timeout_task = asyncio.create_task(
                self._timeout(record, timeout_seconds),
                name=f"jasi-command-timeout-{task_id}",
            )
        return {
            "background_task_id": task_id,
            "command": record.command,
            "status": "running",
            "timeout_seconds": timeout_seconds,
        }

    async def output(
        self,
        task_id: str,
        owner_session_id: str,
        wait_seconds: float,
    ) -> dict[str, Any]:
        record = self._get(task_id, owner_session_id)
        if wait_seconds > 0 and record.pump_task is not None and not record.pump_task.done():
            try:
                await asyncio.wait_for(asyncio.shield(record.pump_task), timeout=wait_seconds)
            except TimeoutError:
                pass
        return self._snapshot(record)

    async def stop(self, task_id: str, owner_session_id: str) -> dict[str, Any]:
        record = self._get(task_id, owner_session_id)
        if record.process.returncode is None:
            record.finish_reason = "stopped"
            _kill_process_tree(record.process)
        if record.pump_task is not None:
            await record.pump_task
        return self._snapshot(record)

    async def aclose(self) -> None:
        active = [record for record in self._tasks.values() if record.process.returncode is None]
        for record in active:
            record.finish_reason = "shutdown"
            _kill_process_tree(record.process)
        pumps = [
            record.pump_task for record in self._tasks.values() if record.pump_task is not None
        ]
        timeouts = [
            record.timeout_task
            for record in self._tasks.values()
            if record.timeout_task is not None
        ]
        for record in self._tasks.values():
            if record.timeout_task is not None and not record.timeout_task.done():
                record.timeout_task.cancel()
        if pumps or timeouts:
            await asyncio.gather(*pumps, *timeouts, return_exceptions=True)
        self._tasks.clear()

    async def _spawn(self, argv: list[str], cwd: Path) -> asyncio.subprocess.Process:
        try:
            return await asyncio.create_subprocess_exec(
                *argv,
                cwd=cwd,
                env=_command_environment(self._workspace.root),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except FileNotFoundError as exc:
            raise ToolRejected(f"command is not installed: {argv[0]}") from exc

    async def _pump(self, record: _CommandTask) -> None:
        assert record.process.stdout is not None
        while True:
            chunk = await record.process.stdout.read(4096)
            if not chunk:
                break
            record.output.extend(chunk)
            if len(record.output) > _MAX_OUTPUT_BYTES:
                record.output_truncated = True
                del record.output[: len(record.output) - _MAX_OUTPUT_BYTES]
        await record.process.wait()
        record.finished_at = time.monotonic()
        if record.finish_reason == "running":
            record.finish_reason = "completed"
        if record.timeout_task is not None and not record.timeout_task.done():
            record.timeout_task.cancel()

    async def _timeout(self, record: _CommandTask, timeout_seconds: int) -> None:
        try:
            await asyncio.sleep(timeout_seconds)
            if record.process.returncode is None:
                record.finish_reason = "timeout"
                _kill_process_tree(record.process)
        except asyncio.CancelledError:
            pass

    def _snapshot(self, record: _CommandTask) -> dict[str, Any]:
        running = record.process.returncode is None
        output = bytes(record.output).decode("utf-8", errors="replace")
        status = "running" if running else record.finish_reason
        if status == "running":
            status = "completed"
        finished_at = record.finished_at or time.monotonic()
        return {
            "background_task_id": record.task_id,
            "command": record.command,
            "description": record.description,
            "status": status,
            "exit_code": record.process.returncode,
            "duration_ms": int((finished_at - record.started_at) * 1000),
            "output": output,
            "truncated": record.output_truncated,
        }

    def _get(self, task_id: str, owner_session_id: str) -> _CommandTask:
        try:
            record = self._tasks[task_id]
        except KeyError as exc:
            raise ToolRejected("background command task was not found") from exc
        if record.owner_session_id != owner_session_id:
            raise ToolRejected("background command task was not found")
        return record

    def _prune(self) -> None:
        completed = [
            task_id
            for task_id, record in self._tasks.items()
            if record.process.returncode is not None
        ]
        for task_id in completed[: max(0, len(self._tasks) - _MAX_BACKGROUND_TASKS + 1)]:
            self._tasks.pop(task_id, None)


def create_shell_tools(
    workspace: FileWorkspace,
    manager: CommandTaskManager,
) -> list[ToolSpec]:
    async def shell(arguments: dict[str, Any], context: ToolExecutionContext) -> ToolOutcome:
        argv = _validate_command(arguments["command"], workspace)
        cwd = workspace.resolve(arguments.get("cwd", "."))
        if not cwd.exists() or not cwd.is_dir():
            raise ToolRejected("command working directory does not exist")
        timeout_specified = "timeout" in arguments
        timeout_seconds = int(arguments.get("timeout", _DEFAULT_TIMEOUT_SECONDS))
        if bool(arguments.get("run_in_background", False)):
            result = await manager.start_background(
                argv=argv,
                cwd=cwd,
                owner_session_id=context.session_id,
                description=arguments["description"],
                timeout_seconds=timeout_seconds if timeout_specified else None,
            )
            return ToolOutcome(
                content=result,
                reveal_tools=("task_output", "task_stop"),
            )
        return ToolOutcome(
            content=await manager.run_foreground(
                argv=argv,
                cwd=cwd,
                timeout_seconds=timeout_seconds,
            )
        )

    async def task_output(
        arguments: dict[str, Any], context: ToolExecutionContext
    ) -> dict[str, Any]:
        return await manager.output(
            arguments["background_task_id"],
            context.session_id,
            float(arguments.get("wait_seconds", 0)),
        )

    async def task_stop(arguments: dict[str, Any], context: ToolExecutionContext) -> dict[str, Any]:
        return await manager.stop(arguments["background_task_id"], context.session_id)

    return [
        ToolSpec(
            name="shell",
            description=(
                "Run one allowlisted read-only command inside the tool workspace. "
                "Shell syntax, pipes, redirects, path traversal, and executable command "
                "options are rejected. Long commands may run in the background."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "minLength": 1},
                    "description": {"type": "string", "minLength": 1, "maxLength": 80},
                    "cwd": {"type": "string", "minLength": 1, "default": "."},
                    "timeout": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": _MAX_TIMEOUT_SECONDS,
                    },
                    "run_in_background": {"type": "boolean", "default": False},
                },
                "required": ["command", "description"],
                "additionalProperties": False,
            },
            risk="external-side-effect",
            handler=shell,
            search_terms=(
                "run command",
                "terminal",
                "shell",
                "命令",
                "终端",
                "运行命令",
                "执行命令",
            ),
        ),
        ToolSpec(
            name="task_output",
            description="Read the current output and status of a background shell task.",
            parameters={
                "type": "object",
                "properties": {
                    "background_task_id": {"type": "string", "minLength": 1},
                    "wait_seconds": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 30,
                        "default": 0,
                    },
                },
                "required": ["background_task_id"],
                "additionalProperties": False,
            },
            risk="read-only",
            handler=task_output,
            search_terms=("background output", "task status", "后台输出", "任务状态"),
        ),
        ToolSpec(
            name="task_stop",
            description="Stop a running background shell task.",
            parameters={
                "type": "object",
                "properties": {
                    "background_task_id": {"type": "string", "minLength": 1},
                },
                "required": ["background_task_id"],
                "additionalProperties": False,
            },
            risk="external-side-effect",
            handler=task_stop,
            search_terms=("stop background task", "cancel command", "停止任务", "终止命令"),
        ),
    ]


def _validate_command(command: str, workspace: FileWorkspace) -> list[str]:
    try:
        argv = shlex.split(command, posix=True)
    except ValueError as exc:
        raise ToolRejected("command has invalid quoting") from exc
    if not argv:
        raise ToolRejected("command cannot be empty")
    executable = argv[0]
    if executable != Path(executable).name or executable not in _ALLOWED_COMMANDS:
        raise ToolRejected(f"command is not allowlisted: {executable}")
    if any(token in _SHELL_OPERATORS or "\n" in token or "\r" in token for token in argv):
        raise ToolRejected("shell operators and multiline commands are not allowed")

    denied_options = {
        "date": ("-s", "--set"),
        "find": (
            "-delete",
            "-exec",
            "-execdir",
            "-fprint",
            "-fprint0",
            "-fprintf",
            "-fls",
            "-ok",
            "-okdir",
            "-L",
            "-H",
        ),
        "grep": ("-R", "--dereference-recursive"),
        "ls": ("-L", "--dereference-command-line"),
        "rg": ("-L", "--follow", "--pre", "--pre-glob"),
        "sort": ("-o", "--output", "--compress-program"),
    }
    forbidden = denied_options.get(executable, ())
    if any(
        token == option or token.startswith(f"{option}=")
        for token in argv[1:]
        for option in forbidden
    ):
        raise ToolRejected(f"command option is not allowed for {executable}")

    denied_compact_options = {
        "date": ("-s",),
        "grep": ("-R",),
        "ls": ("-L",),
        "rg": ("-L",),
        "sort": ("-o",),
    }
    if any(
        token.startswith(option)
        for token in argv[1:]
        for option in denied_compact_options.get(executable, ())
    ):
        raise ToolRejected(f"command option is not allowed for {executable}")

    for token in argv[1:]:
        value = token.split("=", 1)[-1] if "=" in token else token
        if value.startswith("/") or value.startswith("~"):
            raise ToolRejected("absolute and home-relative command paths are not allowed")
        if ".." in Path(value).parts:
            raise ToolRejected("command path traversal is not allowed")
        candidate = workspace.root / value
        if value in {".", "./"} or candidate.exists():
            workspace.resolve(value)
    return argv


def _command_environment(workspace: Path) -> dict[str, str]:
    temporary = workspace / ".tmp"
    temporary.mkdir(parents=True, exist_ok=True)
    return {
        "PATH": _COMMAND_PATH,
        "HOME": str(workspace),
        "TMPDIR": str(temporary),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL", "C.UTF-8"),
    }


def _kill_process_tree(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _bounded_output(raw: bytes) -> tuple[str, bool]:
    truncated = len(raw) > _MAX_OUTPUT_BYTES
    if truncated:
        half = _MAX_OUTPUT_BYTES // 2
        raw = raw[:half] + b"\n...[output truncated]...\n" + raw[-half:]
    return raw.decode("utf-8", errors="replace"), truncated
