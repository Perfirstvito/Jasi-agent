from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ProcessScope = Literal["runtime", "windows"]


@dataclass(frozen=True)
class ProcessRecord:
    pid: int
    name: str
    cpu_seconds: float | None
    memory_bytes: int | None


@dataclass(frozen=True)
class ProcessSnapshot:
    scope: ProcessScope
    processes: tuple[ProcessRecord, ...]


class ProcessLookupUnavailable(RuntimeError):
    pass
