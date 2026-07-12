from __future__ import annotations

import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

_UTC = timezone.utc  # noqa: UP017 - sandbox helper also runs on Python 3.10.


def main(argv: list[str]) -> int:
    workspace = Path(os.environ.get("JASI_WORKSPACE", "/workspace"))
    force, targets = _targets(argv)
    if not targets:
        print("jasi-trash: missing target", file=sys.stderr)
        return 2

    trash_root = workspace / ".jasi-trash"
    batch = trash_root / f"{datetime.now(_UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    failures = 0
    for raw_target in targets:
        target = Path(raw_target)
        absolute = Path(os.path.abspath(target))
        try:
            relative = absolute.relative_to(workspace)
        except ValueError:
            print(f"jasi-trash: outside workspace: {raw_target}", file=sys.stderr)
            failures += 1
            continue
        if relative == Path(".") or relative.parts[:1] == (".jasi-trash",):
            print(f"jasi-trash: protected path: {raw_target}", file=sys.stderr)
            failures += 1
            continue
        if not absolute.exists() and not absolute.is_symlink():
            if not force:
                print(f"jasi-trash: target not found: {raw_target}", file=sys.stderr)
                failures += 1
            continue
        destination = batch / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(absolute), str(destination))
        print(f"trashed {relative} -> {destination.relative_to(workspace)}")
    return 1 if failures else 0


def _targets(argv: list[str]) -> tuple[bool, list[str]]:
    force = False
    parsing_options = True
    targets: list[str] = []
    for value in argv:
        if parsing_options and value == "--":
            parsing_options = False
            continue
        if parsing_options and value.startswith("-") and value != "-":
            force = force or "f" in value or value == "--force"
            continue
        parsing_options = False
        targets.append(value)
    return force, targets


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
