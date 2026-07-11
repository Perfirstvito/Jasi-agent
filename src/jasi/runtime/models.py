from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def add(self, other: Usage) -> Usage:
        return Usage(
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class ModelMessage:
    role: str
    content: str | None = None
    tool_call_id: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    parameters: dict[str, Any]


@dataclass(frozen=True)
class ModelRequest:
    model: str
    messages: list[ModelMessage]
    tools: list[ToolDefinition] = field(default_factory=list)
    timeout_seconds: float = 60.0


@dataclass(frozen=True)
class ModelResponse:
    content: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TurnRequest:
    session_id: str
    inbound_message_id: int
    inbound_text: str
    profile: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ToolExecutionRecord:
    name: str
    arguments: dict[str, Any]
    result: dict[str, Any]
    risk: str
    status: str
    duration_ms: int
    error_message: str | None = None


@dataclass(frozen=True)
class TurnResult:
    turn_id: int
    status: str
    final_text: str
    tool_records: list[ToolExecutionRecord] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    error_code: str | None = None
    error_message: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"


@dataclass(frozen=True)
class TurnStart:
    turn_id: int
    cached_result: TurnResult | None = None
