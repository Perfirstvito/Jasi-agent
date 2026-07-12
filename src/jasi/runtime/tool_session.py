from __future__ import annotations

from dataclasses import dataclass, field

from jasi.runtime.models import ToolGrant
from jasi.runtime.profile import RuntimeProfile


@dataclass
class ToolSession:
    allowed_names: frozenset[str]
    _visible_names: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self._visible_names <= self.allowed_names:
            raise ValueError("visible tools must be allowed")

    @classmethod
    def start(
        cls,
        *,
        profile: RuntimeProfile,
        grant: ToolGrant | None,
        registered_names: frozenset[str],
    ) -> ToolSession:
        granted = profile.allowed_tools
        if grant is not None:
            granted = profile.base_tools | (profile.allowed_tools & grant.tool_names)
        allowed = frozenset(granted & registered_names)
        return cls(
            allowed_names=allowed,
            _visible_names=set(profile.base_tools & allowed),
        )

    @property
    def visible_names(self) -> frozenset[str]:
        return frozenset(self._visible_names)

    def is_allowed(self, name: str) -> bool:
        return name in self.allowed_names

    def reveal(self, names: tuple[str, ...]) -> tuple[str, ...]:
        revealed: list[str] = []
        for name in names:
            if name in self.allowed_names and name not in self._visible_names:
                self._visible_names.add(name)
                revealed.append(name)
        return tuple(revealed)
