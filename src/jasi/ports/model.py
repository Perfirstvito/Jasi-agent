from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from jasi.runtime.models import ModelRequest, ModelResponse


class ModelPort(Protocol):
    async def complete(self, request: ModelRequest) -> ModelResponse: ...
