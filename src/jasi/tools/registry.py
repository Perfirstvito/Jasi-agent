from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from jasi.runtime.errors import ToolFailure, ToolRejected
from jasi.runtime.models import ToolDefinition

ToolHandler = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any] | Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    risk: str
    handler: ToolHandler

    def definition(self) -> ToolDefinition:
        return ToolDefinition(
            name=self.name,
            description=self.description,
            parameters=self.parameters,
        )


class ToolRegistry:
    def __init__(self, tools: list[ToolSpec], max_result_chars: int = 4000) -> None:
        self._tools = {tool.name: tool for tool in tools}
        self._max_result_chars = max_result_chars

    def definitions(self, allowed_tools: frozenset[str]) -> list[ToolDefinition]:
        return [tool.definition() for name, tool in self._tools.items() if name in allowed_tools]

    def get(self, name: str) -> ToolSpec:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise ToolRejected(f"unknown tool: {name}") from exc

    async def execute(
        self, name: str, arguments: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        tool = self.get(name)
        if not isinstance(arguments, dict):
            raise ToolRejected("tool arguments must be an object")
        try:
            value = tool.handler(arguments, context)
            result = await value if inspect.isawaitable(value) else value
        except ToolRejected:
            raise
        except Exception as exc:
            raise ToolFailure(f"{name} failed: {exc}") from exc
        if not isinstance(result, dict):
            result = {"value": result}
        return self._truncate(result)

    def risk(self, name: str) -> str:
        return self.get(name).risk

    def _truncate(self, result: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(result, ensure_ascii=False, default=str)
        if len(encoded) <= self._max_result_chars:
            return result
        return {
            "truncated": True,
            "content": encoded[: self._max_result_chars],
        }
