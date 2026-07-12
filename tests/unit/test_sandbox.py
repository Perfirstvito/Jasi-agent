from __future__ import annotations

from pathlib import Path

import pytest

from jasi.runtime.errors import ToolRejected
from jasi.tools.filesystem import FileWorkspace
from jasi.tools.sandbox import BubblewrapSandbox


def test_bubblewrap_launch_contains_hard_boundaries_and_no_host_secrets(
    tmp_path: Path,
) -> None:
    workspace = FileWorkspace(tmp_path / "workspace")
    sandbox = BubblewrapSandbox(workspace)
    if not sandbox.available:
        pytest.skip(sandbox.unavailable_reason or "sandbox unavailable")

    launch = sandbox.build_launch("env", workspace.root, 60)
    joined = " ".join(launch.argv)

    assert "--unshare-all" in launch.argv
    assert "--share-net" not in launch.argv
    assert "--clearenv" in launch.argv
    assert "--nproc=1024:1024" in launch.argv
    assert str(workspace.root) in launch.argv
    assert "/mnt/d/WorkSpace/myProject/agent/Jasi/.env" not in joined
    assert all("API_KEY" not in value and "BOT_TOKEN" not in value for value in launch.env)


def test_bubblewrap_has_no_unsafe_cross_platform_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = FileWorkspace(tmp_path / "workspace")
    monkeypatch.setattr("jasi.tools.sandbox.platform.system", lambda: "Windows")
    sandbox = BubblewrapSandbox(workspace)

    assert sandbox.available is False
    with pytest.raises(ToolRejected, match="requires Linux or WSL"):
        sandbox.build_launch("echo unsafe", workspace.root, 60)
