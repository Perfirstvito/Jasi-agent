from __future__ import annotations

from typing import Any

from jasi.runtime.errors import ToolRejected
from jasi.tools.registry import ToolExecutionContext, ToolOutcome, ToolRegistry, ToolSpec


def create_tool_search_tool(registry: ToolRegistry) -> ToolSpec:
    def search_tools(arguments: dict[str, Any], context: ToolExecutionContext) -> ToolOutcome:
        query = str(arguments["query"]).strip()
        if not query:
            raise ToolRejected("tool search query cannot be empty")
        limit = int(arguments.get("limit", 5))
        candidates = context.allowed_tools - context.visible_tools
        matches = registry.search(query, candidates=candidates, limit=limit)
        names = tuple(tool.name for tool in matches)
        content: dict[str, Any] = {
            "matched": [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "risk": tool.risk,
                    "source": {
                        "type": tool.source_type,
                        "name": tool.source_name,
                    },
                }
                for tool in matches
            ],
        }
        if names:
            content["next_action"] = (
                "These tools will be available on the next model step. "
                "Call the one needed for the task."
            )
        else:
            content["next_action"] = "No authorized hidden tool matched."
        return ToolOutcome(content=content, reveal_tools=names)

    return ToolSpec(
        name="tool_search",
        description=(
            "Find additional tools that are already authorized for this task. "
            "Use it when the currently visible tools cannot complete the request. "
            "Matched tools become available on the next model step."
        ),
        parameters={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "minLength": 1,
                    "description": "A short description of the capability needed.",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5,
                    "default": 5,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        risk="read-only",
        handler=search_tools,
        search_terms=("find tools", "discover tools", "工具搜索", "查找工具"),
    )
