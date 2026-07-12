from __future__ import annotations

from jasi.tools.registry import ToolRegistry
from jasi.tools.search import create_tool_search_tool
from jasi.tools.time import get_current_time_tool


def build_builtin_tool_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(get_current_time_tool)
    registry.register(create_tool_search_tool(registry))
    return registry
