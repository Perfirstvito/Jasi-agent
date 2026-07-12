from __future__ import annotations

import inspect
import json
import logging
import re
from collections.abc import Awaitable, Callable, Collection, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from jasi.runtime.errors import ToolFailure, ToolRejected
from jasi.runtime.models import ToolDefinition

logger = logging.getLogger(__name__)

_TOOL_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass(frozen=True)
class ToolExecutionContext:
    work_id: int
    session_id: str
    conversation_id: int
    profile: str
    timezone: str
    allowed_tools: frozenset[str]
    visible_tools: frozenset[str]
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class ToolOutcome:
    content: dict[str, Any]
    reveal_tools: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.content, dict):
            raise TypeError("tool outcome content must be an object")
        names: list[str] = []
        seen: set[str] = set()
        for name in self.reveal_tools:
            if not isinstance(name, str) or not _TOOL_NAME_PATTERN.fullmatch(name):
                raise ValueError(f"invalid revealed tool name: {name!r}")
            if name not in seen:
                names.append(name)
                seen.add(name)
        object.__setattr__(self, "content", dict(self.content))
        object.__setattr__(self, "reveal_tools", tuple(names))


ToolHandlerResult = dict[str, Any] | ToolOutcome
ToolHandler = Callable[
    [dict[str, Any], ToolExecutionContext],
    ToolHandlerResult | Awaitable[ToolHandlerResult],
]


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    parameters: dict[str, Any]
    risk: str
    handler: ToolHandler
    source_type: str = "builtin"
    source_name: str = "jasi"
    search_terms: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not _TOOL_NAME_PATTERN.fullmatch(self.name):
            raise ValueError(f"invalid tool name: {self.name!r}")
        if not self.description.strip():
            raise ValueError(f"tool description cannot be empty: {self.name}")
        if not self.risk.strip():
            raise ValueError(f"tool risk cannot be empty: {self.name}")
        if not self.source_type.strip() or not self.source_name.strip():
            raise ValueError(f"tool source cannot be empty: {self.name}")
        if not callable(self.handler):
            raise TypeError(f"tool handler must be callable: {self.name}")
        if not isinstance(self.parameters, dict) or self.parameters.get("type") != "object":
            raise ValueError(f"tool parameters must be an object schema: {self.name}")
        try:
            Draft202012Validator.check_schema(self.parameters)
        except SchemaError as exc:
            raise ValueError(f"invalid tool schema: {self.name}") from exc
        if any(not isinstance(term, str) or not term.strip() for term in self.search_terms):
            raise ValueError(f"tool search terms must be non-empty strings: {self.name}")
        object.__setattr__(self, "parameters", deepcopy(self.parameters))
        object.__setattr__(
            self,
            "search_terms",
            tuple(dict.fromkeys(term.strip() for term in self.search_terms)),
        )

    def definition(self) -> ToolDefinition:
        return ToolDefinition(
            name=self.name,
            description=self.description,
            parameters=deepcopy(self.parameters),
        )


class ToolRegistry:
    def __init__(self, tools: Collection[ToolSpec] = (), max_result_chars: int = 4000) -> None:
        if max_result_chars <= 0:
            raise ValueError("max tool result chars must be positive")
        self._tools: dict[str, ToolSpec] = {}
        self._validators: dict[str, Draft202012Validator] = {}
        self._max_result_chars = max_result_chars
        for tool in tools:
            self.register(tool)

    def register(self, tool: ToolSpec) -> None:
        if tool.name in self._tools:
            raise ValueError(f"duplicate tool: {tool.name}")
        self._tools[tool.name] = tool
        self._validators[tool.name] = Draft202012Validator(tool.parameters)

    @property
    def registered_names(self) -> frozenset[str]:
        return frozenset(self._tools)

    def definitions(self, names: Collection[str]) -> list[ToolDefinition]:
        selected = set(names)
        return [tool.definition() for name, tool in self._tools.items() if name in selected]

    def get(self, name: str) -> ToolSpec:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise ToolRejected(f"unknown tool: {name}") from exc

    async def execute(
        self,
        name: str,
        arguments: dict[str, Any],
        context: ToolExecutionContext,
    ) -> ToolOutcome:
        tool = self.get(name)
        if not isinstance(arguments, dict):
            raise ToolRejected("tool arguments must be an object")
        errors = sorted(
            self._validators[name].iter_errors(arguments),
            key=lambda error: tuple(str(part) for part in error.absolute_path),
        )
        if errors:
            error = errors[0]
            path = ".".join(str(part) for part in error.absolute_path) or "arguments"
            raise ToolRejected(f"invalid arguments for {name} at {path} ({error.validator})")
        try:
            value = tool.handler(arguments, context)
            result = await value if inspect.isawaitable(value) else value
        except ToolRejected:
            raise
        except Exception as exc:
            logger.error(
                "tool execution failed tool=%s error=%s",
                name,
                exc.__class__.__name__,
            )
            raise ToolFailure(f"{name} failed") from exc
        if isinstance(result, dict):
            outcome = ToolOutcome(content=result)
        elif isinstance(result, ToolOutcome):
            outcome = result
        else:
            raise ToolFailure(f"{name} returned an invalid result")
        return ToolOutcome(
            content=self._truncate(outcome.content),
            reveal_tools=outcome.reveal_tools,
        )

    def risk(self, name: str) -> str:
        return self.get(name).risk

    def search(
        self,
        query: str,
        *,
        candidates: Collection[str],
        limit: int = 5,
    ) -> list[ToolSpec]:
        normalized = query.casefold().strip()
        if not normalized or limit <= 0:
            return []
        candidate_names = set(candidates)
        matches: list[tuple[int, str, ToolSpec]] = []
        for name, tool in self._tools.items():
            if name not in candidate_names:
                continue
            score = _search_score(tool, normalized)
            if score > 0:
                matches.append((score, name, tool))
        matches.sort(key=lambda item: (-item[0], item[1]))
        return [tool for _, _, tool in matches[:limit]]

    def _truncate(self, result: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(result, ensure_ascii=False, default=str)
        if len(encoded) <= self._max_result_chars:
            return result
        return {
            "truncated": True,
            "content": encoded[: self._max_result_chars],
        }


def _search_score(tool: ToolSpec, query: str) -> int:
    name = tool.name.casefold()
    description = tool.description.casefold()
    term_values = tuple(term.casefold() for term in tool.search_terms)
    terms = " ".join(term_values)
    if query == name:
        return 100

    score = 0
    if query in name:
        score += 40
    if query in terms:
        score += 20
    if query in description:
        score += 10
    if any(term in query for term in term_values):
        score += 10

    tokens = {token for token in re.split(r"[\s_\-]+", query) if token}
    for token in tokens:
        if token in name:
            score += 8
        if token in terms:
            score += 4
        if token in description:
            score += 2
    return score
