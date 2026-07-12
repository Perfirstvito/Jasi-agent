from __future__ import annotations

import asyncio
import os
import signal
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from jasi.runtime.errors import ToolRejected
from jasi.tools.command_policy import (
    CommandPolicyPipeline,
    CommandPolicyRejected,
    CommandPolicyState,
    build_default_command_policy,
)
from jasi.tools.filesystem import FileWorkspace
from jasi.tools.registry import ToolExecutionContext, ToolOutcome, ToolSpec
from jasi.tools.sandbox import BubblewrapSandbox, CommandSandbox

_MAX_OUTPUT_BYTES = 30_000
_MAX_BACKGROUND_TASKS = 32
_DEFAULT_TIMEOUT_SECONDS = 60
_DEFAULT_BACKGROUND_TIMEOUT_SECONDS = 3600
_MAX_TIMEOUT_SECONDS = 3600


@dataclass
class _CommandTask:
    task_id: str
    owner_session_id: str
    process: asyncio.subprocess.Process
    command: str
    executed_command: str
    description: str
    started_at: float
    output: bytearray
    pump_task: asyncio.Task[None] | None = None
    timeout_task: asyncio.Task[None] | None = None
    finish_reason: str = "running"
    output_truncated: bool = False
    finished_at: float | None = None


class CommandTaskManager:
    def __init__(
        self,
        workspace: FileWorkspace,
        *,
        sandbox: CommandSandbox | None = None,
    ) -> None:
        self._workspace = workspace
        self._sandbox = sandbox or BubblewrapSandbox(workspace)
        self._tasks: dict[str, _CommandTask] = {}

    @property
    def sandbox_available(self) -> bool:
        return self._sandbox.available

    @property
    def sandbox_unavailable_reason(self) -> str | None:
        return self._sandbox.unavailable_reason

    async def run_foreground(
        self,
        *,
        command: str,
        display_command: str,
        cwd: Path,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        process = await self._spawn(command, cwd, timeout_seconds)
        started = time.monotonic()
        timed_out = False
        capture_task = asyncio.create_task(
            _capture_output(process),
            name=f"jasi-command-capture-{process.pid}",
        )
        completion = asyncio.gather(process.wait(), capture_task)
        try:
            await asyncio.wait_for(asyncio.shield(completion), timeout=timeout_seconds)
        except TimeoutError:
            timed_out = True
            _kill_process_tree(process)
            await completion
        except asyncio.CancelledError:
            _kill_process_tree(process)
            await asyncio.shield(completion)
            raise
        output, truncated = capture_task.result()
        return {
            "command": display_command,
            "executed_command": command,
            "exit_code": process.returncode,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "timed_out": timed_out,
            "output": output,
            "truncated": truncated,
        }

    async def start_background(
        self,
        *,
        command: str,
        display_command: str,
        cwd: Path,
        owner_session_id: str,
        description: str,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        self._prune()
        if len(self._tasks) >= _MAX_BACKGROUND_TASKS:
            raise ToolRejected("too many background command tasks")
        process = await self._spawn(command, cwd, timeout_seconds)
        task_id = uuid4().hex[:12]
        record = _CommandTask(
            task_id=task_id,
            owner_session_id=owner_session_id,
            process=process,
            command=display_command,
            executed_command=command,
            description=description,
            started_at=time.monotonic(),
            output=bytearray(),
        )
        self._tasks[task_id] = record
        record.pump_task = asyncio.create_task(
            self._pump(record),
            name=f"jasi-command-{task_id}",
        )
        record.timeout_task = asyncio.create_task(
            self._timeout(record, timeout_seconds),
            name=f"jasi-command-timeout-{task_id}",
        )
        return {
            "background_task_id": task_id,
            "command": record.command,
            "executed_command": record.executed_command,
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
        for task in timeouts:
            if not task.done():
                task.cancel()
        if pumps or timeouts:
            await asyncio.gather(*pumps, *timeouts, return_exceptions=True)
        self._tasks.clear()

    async def _spawn(
        self,
        command: str,
        cwd: Path,
        timeout_seconds: int,
    ) -> asyncio.subprocess.Process:
        launch = self._sandbox.build_launch(command, cwd, timeout_seconds)
        try:
            return await asyncio.create_subprocess_exec(
                *launch.argv,
                cwd=launch.cwd,
                env=dict(launch.env),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except FileNotFoundError as exc:
            raise ToolRejected("command sandbox executable is unavailable") from exc
        except PermissionError as exc:
            raise ToolRejected("command sandbox could not be started") from exc

    async def _pump(self, record: _CommandTask) -> None:
        assert record.process.stdout is not None
        while True:
            chunk = await record.process.stdout.read(4096)
            if not chunk:
                break
            record.output_truncated = (
                _append_bounded(record.output, chunk, preserve_head=False)
                or record.output_truncated
            )
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
        finished_at = record.finished_at or time.monotonic()
        return {
            "background_task_id": record.task_id,
            "command": record.command,
            "executed_command": record.executed_command,
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
    *,
    policy: CommandPolicyPipeline | None = None,
) -> list[ToolSpec]:
    command_policy = policy or build_default_command_policy()

    async def shell(arguments: dict[str, Any], context: ToolExecutionContext) -> ToolOutcome:
        decision = _evaluate(command_policy, arguments["command"])
        cwd = workspace.resolve(arguments.get("cwd", "."))
        if not cwd.exists() or not cwd.is_dir():
            raise ToolRejected("command working directory does not exist")
        background = bool(arguments.get("run_in_background", False))
        default_timeout = (
            _DEFAULT_BACKGROUND_TIMEOUT_SECONDS if background else _DEFAULT_TIMEOUT_SECONDS
        )
        timeout_seconds = int(arguments.get("timeout", default_timeout))
        if background:
            result = await manager.start_background(
                command=decision.command,
                display_command=decision.original_command,
                cwd=cwd,
                owner_session_id=context.session_id,
                description=arguments["description"],
                timeout_seconds=timeout_seconds,
            )
            return ToolOutcome(
                content=_with_policy(result, decision),
                reveal_tools=("task_output", "task_stop"),
            )
        result = await manager.run_foreground(
            command=decision.command,
            display_command=decision.original_command,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
        )
        return ToolOutcome(content=_with_policy(result, decision))

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
                "Run a full Bash command inside an isolated workspace sandbox. Pipes, redirects, "
                "subcommands, interpreters, and ordinary system tools are supported. The project, "
                "host filesystem, secrets, and host processes are not mounted; network is disabled "
                "unless enabled by the operator. Standard deletion commands are rewritten to the "
                "workspace .jasi-trash directory. Long commands may run in the background."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "minLength": 1, "maxLength": 20_000},
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
                "bash",
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
            description="Stop a running background shell task and its sandboxed process tree.",
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


def _evaluate(policy: CommandPolicyPipeline, command: str) -> CommandPolicyState:
    try:
        return policy.evaluate(command)
    except CommandPolicyRejected as exc:
        raise ToolRejected(str(exc)) from exc


def _with_policy(result: dict[str, Any], decision: CommandPolicyState) -> dict[str, Any]:
    return {
        **result,
        "policy": {
            "risk": decision.risk,
            "rewrites": [asdict(rewrite) for rewrite in decision.rewrites],
        },
    }


def _kill_process_tree(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def _capture_output(process: asyncio.subprocess.Process) -> tuple[str, bool]:
    assert process.stdout is not None
    captured = bytearray()
    truncated = False
    while True:
        chunk = await process.stdout.read(4096)
        if not chunk:
            break
        truncated = _append_bounded(captured, chunk, preserve_head=True) or truncated
    raw = bytes(captured)
    if truncated:
        half = _MAX_OUTPUT_BYTES // 2
        raw = raw[:half] + b"\n...[output truncated]...\n" + raw[-half:]
    return raw.decode("utf-8", errors="replace"), truncated


def _append_bounded(buffer: bytearray, chunk: bytes, *, preserve_head: bool) -> bool:
    buffer.extend(chunk)
    if len(buffer) <= _MAX_OUTPUT_BYTES:
        return False
    if preserve_head:
        half = _MAX_OUTPUT_BYTES // 2
        buffer[:] = buffer[:half] + buffer[-half:]
    else:
        del buffer[: len(buffer) - _MAX_OUTPUT_BYTES]
    return True
