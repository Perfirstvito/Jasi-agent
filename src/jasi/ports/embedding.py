from __future__ import annotations

from typing import Protocol


class EmbeddingPort(Protocol):
    @property
    def model_name(self) -> str: ...

    async def embed(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]: ...
