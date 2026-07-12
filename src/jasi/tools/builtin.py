from __future__ import annotations

from pathlib import Path

from jasi.ports.tools import MessageLookupPort, ProcessLookupPort
from jasi.tools.filesystem import FileWorkspace, create_filesystem_tools
from jasi.tools.messages import create_message_tools
from jasi.tools.processes import create_process_tools
from jasi.tools.registry import ToolRegistry
from jasi.tools.search import create_tool_search_tool
from jasi.tools.shell import CommandTaskManager, create_shell_tools
from jasi.tools.time import get_current_time_tool
from jasi.tools.web import create_web_tools


def build_builtin_tool_registry(
    *,
    workspace: Path | FileWorkspace = Path("workspace/tools"),
    messages: MessageLookupPort | None = None,
    processes: ProcessLookupPort | None = None,
    command_tasks: CommandTaskManager | None = None,
    allow_fake_ip_dns: bool = False,
) -> ToolRegistry:
    file_workspace = workspace if isinstance(workspace, FileWorkspace) else FileWorkspace(workspace)
    task_manager = command_tasks or CommandTaskManager(file_workspace)
    registry = ToolRegistry()
    registry.register(get_current_time_tool)
    registry.register(create_tool_search_tool(registry))
    for tool in create_web_tools(allow_fake_ip_dns=allow_fake_ip_dns):
        registry.register(tool)
    for tool in create_filesystem_tools(file_workspace):
        registry.register(tool)
    for tool in create_shell_tools(file_workspace, task_manager):
        registry.register(tool)
    if messages is not None:
        for tool in create_message_tools(messages):
            registry.register(tool)
    if processes is not None:
        for tool in create_process_tools(processes):
            registry.register(tool)
    return registry
