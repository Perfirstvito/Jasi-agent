from __future__ import annotations

import asyncio
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from jasi.runtime.errors import ToolRejected
from jasi.tools.registry import ToolExecutionContext, ToolSpec

_MAX_READ_BYTES = 1_000_000
_MAX_READ_LINES = 400
_DEFAULT_READ_LINES = 200
_MAX_WRITE_CHARS = 100_000
_MAX_DIRECTORY_ENTRIES = 200


class FileWorkspace:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._mutation_locks: dict[Path, asyncio.Lock] = {}

    def resolve(self, raw_path: str) -> Path:
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ToolRejected("path cannot be empty")
        supplied = Path(raw_path.strip()).expanduser()
        candidate = supplied if supplied.is_absolute() else self.root / supplied
        resolved = candidate.resolve(strict=False)
        if not resolved.is_relative_to(self.root):
            raise ToolRejected("path is outside the tool workspace")
        return resolved

    def display(self, path: Path) -> str:
        return str(path.relative_to(self.root)) or "."

    def mutation_lock(self, path: Path) -> asyncio.Lock:
        return self._mutation_locks.setdefault(path, asyncio.Lock())


def create_filesystem_tools(workspace: FileWorkspace) -> list[ToolSpec]:
    async def read_file(
        arguments: dict[str, Any], _context: ToolExecutionContext
    ) -> dict[str, Any]:
        path = workspace.resolve(arguments["path"])
        offset = int(arguments.get("offset", 0))
        limit = int(arguments.get("limit", _DEFAULT_READ_LINES))
        return await asyncio.to_thread(_read_text_file, workspace, path, offset, limit)

    async def list_dir(arguments: dict[str, Any], _context: ToolExecutionContext) -> dict[str, Any]:
        path = workspace.resolve(arguments.get("path", "."))
        return await asyncio.to_thread(_list_directory, workspace, path)

    async def write_file(
        arguments: dict[str, Any], _context: ToolExecutionContext
    ) -> dict[str, Any]:
        path = workspace.resolve(arguments["path"])
        content = arguments["content"]
        async with workspace.mutation_lock(path):
            return await asyncio.to_thread(_write_text_file, workspace, path, content)

    async def edit_file(
        arguments: dict[str, Any], _context: ToolExecutionContext
    ) -> dict[str, Any]:
        path = workspace.resolve(arguments["path"])
        async with workspace.mutation_lock(path):
            return await asyncio.to_thread(
                _edit_text_file,
                workspace,
                path,
                arguments["old_text"],
                arguments["new_text"],
                bool(arguments.get("replace_all", False)),
            )

    return [
        ToolSpec(
            name="read_file",
            description=(
                "Read a UTF-8 text file inside the configured tool workspace. "
                "The result includes line numbers and supports offset/limit pagination."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "offset": {"type": "integer", "minimum": 0, "default": 0},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": _MAX_READ_LINES,
                        "default": _DEFAULT_READ_LINES,
                    },
                },
                "required": ["path"],
                "additionalProperties": False,
            },
            risk="read-only",
            handler=read_file,
            search_terms=("read file", "file content", "读取文件", "查看文件"),
        ),
        ToolSpec(
            name="list_dir",
            description="List files and directories inside the configured tool workspace.",
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "minLength": 1, "default": "."},
                },
                "additionalProperties": False,
            },
            risk="read-only",
            handler=list_dir,
            search_terms=("list directory", "files", "目录", "文件列表"),
        ),
        ToolSpec(
            name="write_file",
            description=(
                "Create or fully overwrite a UTF-8 text file inside the tool workspace. "
                "Use edit_file for a targeted change to an existing file."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "content": {"type": "string", "maxLength": _MAX_WRITE_CHARS},
                },
                "required": ["path", "content"],
                "additionalProperties": False,
            },
            risk="write",
            handler=write_file,
            search_terms=("write file", "create file", "写文件", "创建文件"),
        ),
        ToolSpec(
            name="edit_file",
            description=(
                "Replace an exact text fragment in an existing UTF-8 file inside the tool "
                "workspace. The old text must match exactly."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "old_text": {"type": "string", "minLength": 1},
                    "new_text": {"type": "string", "maxLength": _MAX_WRITE_CHARS},
                    "replace_all": {"type": "boolean", "default": False},
                },
                "required": ["path", "old_text", "new_text"],
                "additionalProperties": False,
            },
            risk="write",
            handler=edit_file,
            search_terms=("edit file", "replace text", "修改文件", "替换文本"),
        ),
    ]


def _read_text_file(
    workspace: FileWorkspace,
    path: Path,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    if not path.exists():
        raise ToolRejected("file does not exist")
    if not path.is_file():
        raise ToolRejected("path is not a file")
    size = path.stat().st_size
    if size > _MAX_READ_BYTES:
        raise ToolRejected(f"file exceeds the {_MAX_READ_BYTES}-byte read limit")
    raw = path.read_bytes()
    if b"\x00" in raw[:4096]:
        raise ToolRejected("binary files are not supported")
    decoded = raw.decode("utf-8", errors="replace")
    lines = decoded.splitlines()
    selected = lines[offset : offset + limit]
    numbered = "\n".join(
        f"{line_number:6}: {line}" for line_number, line in enumerate(selected, start=offset + 1)
    )
    return {
        "path": workspace.display(path),
        "content": numbered,
        "start_line": offset + 1 if selected else None,
        "end_line": offset + len(selected) if selected else None,
        "total_lines": len(lines),
        "size_bytes": size,
        "truncated": offset + len(selected) < len(lines),
        "decode_replacements": "\ufffd" in decoded,
    }


def _list_directory(workspace: FileWorkspace, path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ToolRejected("directory does not exist")
    if not path.is_dir():
        raise ToolRejected("path is not a directory")
    children = sorted(path.iterdir(), key=lambda item: (not item.is_dir(), item.name.casefold()))
    entries = [_directory_entry(item) for item in children[:_MAX_DIRECTORY_ENTRIES]]
    return {
        "path": workspace.display(path),
        "entries": entries,
        "count": len(entries),
        "truncated": len(children) > len(entries),
    }


def _directory_entry(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        return {"name": path.name, "type": "symlink", "size_bytes": None}
    if path.is_dir():
        return {"name": path.name, "type": "directory", "size_bytes": None}
    return {"name": path.name, "type": "file", "size_bytes": path.stat().st_size}


def _write_text_file(
    workspace: FileWorkspace,
    path: Path,
    content: str,
) -> dict[str, Any]:
    if path.exists() and not path.is_file():
        raise ToolRejected("path is not a file")
    existed = path.exists()
    _atomic_write(path, content)
    return {
        "path": workspace.display(path),
        "created": not existed,
        "characters_written": len(content),
    }


def _edit_text_file(
    workspace: FileWorkspace,
    path: Path,
    old_text: str,
    new_text: str,
    replace_all: bool,
) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ToolRejected("file does not exist")
    if path.stat().st_size > _MAX_READ_BYTES:
        raise ToolRejected(f"file exceeds the {_MAX_READ_BYTES}-byte edit limit")
    content = path.read_text(encoding="utf-8")
    matches = content.count(old_text)
    if matches == 0:
        raise ToolRejected("old_text was not found")
    if matches > 1 and not replace_all:
        raise ToolRejected("old_text matches more than once; add context or use replace_all")
    updated = (
        content.replace(old_text, new_text)
        if replace_all
        else content.replace(old_text, new_text, 1)
    )
    _atomic_write(path, updated)
    return {
        "path": workspace.display(path),
        "replacements": matches if replace_all else 1,
        "characters_written": len(updated),
    }


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    previous_mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else None
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            temporary_name = stream.name
        if previous_mode is not None:
            os.chmod(temporary_name, previous_mode)
        os.replace(temporary_name, path)
    finally:
        if temporary_name:
            try:
                Path(temporary_name).unlink(missing_ok=True)
            except OSError:
                pass
