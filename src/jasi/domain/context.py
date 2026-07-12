from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ContextTrust = Literal["trusted", "derived", "untrusted"]


@dataclass(frozen=True)
class HistoryItem:
    message_id: int
    sequence: int
    role: Literal["user", "assistant"]
    content: str


@dataclass(frozen=True)
class ContextItem:
    kind: str
    content: str
    trust: ContextTrust
    references: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.kind.strip():
            raise ValueError("context item kind cannot be empty")
        if not self.content.strip():
            raise ValueError("context item content cannot be empty")


@dataclass(frozen=True)
class TurnContextQuery:
    conversation_id: int
    before_sequence: int | None
    input_text: str
    profile: str
    history_limit: int
    turn_id: int
    include_memory: bool


@dataclass(frozen=True)
class TurnContextSnapshot:
    history: tuple[HistoryItem, ...] = ()
    context_items: tuple[ContextItem, ...] = ()
