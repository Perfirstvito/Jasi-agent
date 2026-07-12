from __future__ import annotations

import platform
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Protocol

from jasi.runtime.errors import ToolRejected
from jasi.tools.filesystem import FileWorkspace

_SANDBOX_ROOT = PurePosixPath("/workspace")
_MAX_ADDRESS_SPACE_BYTES = 2 * 1024 * 1024 * 1024
_MAX_FILE_BYTES = 128 * 1024 * 1024
_MAX_OPEN_FILES = 256
_MAX_PROCESSES = 1024


@dataclass(frozen=True)
class SandboxLaunch:
    argv: tuple[str, ...]
    cwd: Path | None
    env: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))


class CommandSandbox(Protocol):
    @property
    def available(self) -> bool: ...

    @property
    def unavailable_reason(self) -> str | None: ...

    def build_launch(
        self,
        command: str,
        cwd: Path,
        timeout_seconds: int,
    ) -> SandboxLaunch: ...


class BubblewrapSandbox:
    def __init__(
        self,
        workspace: FileWorkspace,
        *,
        allow_network: bool = False,
    ) -> None:
        self._workspace = workspace
        self._allow_network = allow_network
        self._bwrap = shutil.which("bwrap") if platform.system() == "Linux" else None
        self._prlimit = shutil.which("prlimit") if platform.system() == "Linux" else None
        self._trash_script = Path(__file__).with_name("scripts") / "jasi_trash.py"

    @property
    def available(self) -> bool:
        return bool(
            self._bwrap
            and self._prlimit
            and Path("/usr/bin/bash").is_file()
            and self._trash_script.is_file()
        )

    @property
    def unavailable_reason(self) -> str | None:
        if self.available:
            return None
        if platform.system() != "Linux":
            return "sandboxed shell requires Linux or WSL"
        if not self._bwrap:
            return "bubblewrap is not installed"
        if not self._prlimit:
            return "prlimit is not installed"
        if not Path("/usr/bin/bash").is_file():
            return "/usr/bin/bash is unavailable"
        return "soft-delete helper is unavailable"

    def build_launch(
        self,
        command: str,
        cwd: Path,
        timeout_seconds: int,
    ) -> SandboxLaunch:
        if not self.available:
            raise ToolRejected(self.unavailable_reason or "command sandbox is unavailable")
        resolved_cwd = self._workspace.resolve(str(cwd))
        if not resolved_cwd.exists() or not resolved_cwd.is_dir():
            raise ToolRejected("command working directory does not exist")
        relative_cwd = resolved_cwd.relative_to(self._workspace.root)
        sandbox_cwd = str(_SANDBOX_ROOT / PurePosixPath(relative_cwd.as_posix()))
        cpu_seconds = max(10, timeout_seconds + 30)

        argv = [
            str(self._prlimit),
            f"--as={_MAX_ADDRESS_SPACE_BYTES}:{_MAX_ADDRESS_SPACE_BYTES}",
            f"--fsize={_MAX_FILE_BYTES}:{_MAX_FILE_BYTES}",
            f"--nofile={_MAX_OPEN_FILES}:{_MAX_OPEN_FILES}",
            f"--nproc={_MAX_PROCESSES}:{_MAX_PROCESSES}",
            "--core=0:0",
            f"--cpu={cpu_seconds}:{cpu_seconds}",
            "--",
            str(self._bwrap),
            "--die-with-parent",
            "--new-session",
            "--unshare-all",
        ]
        if self._allow_network:
            argv.append("--share-net")
        argv.extend(
            [
                "--hostname",
                "jasi-sandbox",
                "--cap-drop",
                "ALL",
                "--ro-bind",
                "/usr",
                "/usr",
                "--symlink",
                "usr/bin",
                "/bin",
                "--symlink",
                "usr/lib",
                "/lib",
                "--symlink",
                "usr/lib64",
                "/lib64",
                "--symlink",
                "usr/sbin",
                "/sbin",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--tmpfs",
                "/tmp",
                "--dir",
                "/home",
                "--dir",
                "/etc",
                "--ro-bind-try",
                "/etc/ld.so.cache",
                "/etc/ld.so.cache",
                "--ro-bind-try",
                "/etc/passwd",
                "/etc/passwd",
                "--ro-bind-try",
                "/etc/group",
                "/etc/group",
                "--dir",
                "/jasi",
                "--dir",
                "/jasi/bin",
                "--ro-bind",
                str(self._trash_script),
                "/jasi/bin/jasi_trash.py",
                "--bind",
                str(self._workspace.root),
                str(_SANDBOX_ROOT),
            ]
        )
        if self._allow_network:
            argv.extend(_network_mounts())
        argv.extend(
            [
                "--chdir",
                sandbox_cwd,
                "--clearenv",
                "--setenv",
                "HOME",
                str(_SANDBOX_ROOT),
                "--setenv",
                "JASI_WORKSPACE",
                str(_SANDBOX_ROOT),
                "--setenv",
                "PATH",
                "/usr/bin:/bin:/usr/sbin:/sbin",
                "--setenv",
                "LANG",
                "C.UTF-8",
                "--setenv",
                "LC_ALL",
                "C.UTF-8",
                "--",
                "/usr/bin/bash",
                "--noprofile",
                "--norc",
                "-o",
                "pipefail",
                "-c",
                command,
            ]
        )
        return SandboxLaunch(
            argv=tuple(argv),
            cwd=None,
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
        )


def _network_mounts() -> list[str]:
    mounts: list[str] = []
    for source in (
        "/etc/hosts",
        "/etc/nsswitch.conf",
        "/etc/resolv.conf",
        "/etc/ssl",
    ):
        if Path(source).exists():
            mounts.extend(("--ro-bind", source, source))
    return mounts
