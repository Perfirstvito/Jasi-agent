from __future__ import annotations

import os
import tempfile
import threading
from pathlib import Path

from jasi.domain.memory import (
    MEMORY_DOCUMENT_NAMES,
    MEMORY_DOCUMENT_TEMPLATES,
    MemoryDocumentName,
    MemoryDocumentSnapshot,
    MemoryWorkspaceSnapshot,
    content_hash,
)


class MemoryDocumentConflict(RuntimeError):
    pass


class MarkdownMemoryStore:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._locks_guard = threading.Lock()
        self._locks: dict[str, threading.RLock] = {}

    def ensure_workspace(self, scope_directory: str) -> MemoryWorkspaceSnapshot:
        directory = self._scope_path(scope_directory)
        lock = self._lock(scope_directory)
        with lock:
            directory.mkdir(parents=True, exist_ok=True)
            for name in MEMORY_DOCUMENT_NAMES:
                path = directory / name
                if not path.exists():
                    self._atomic_write(path, MEMORY_DOCUMENT_TEMPLATES[name])
            return self._read_workspace_unlocked(scope_directory)

    def read_workspace(self, scope_directory: str) -> MemoryWorkspaceSnapshot:
        lock = self._lock(scope_directory)
        with lock:
            return self._read_workspace_unlocked(scope_directory)

    def write_document(
        self,
        scope_directory: str,
        name: MemoryDocumentName,
        content: str,
        *,
        expected_hash: str | None = None,
    ) -> MemoryDocumentSnapshot:
        lock = self._lock(scope_directory)
        with lock:
            path = self._scope_path(scope_directory) / name
            current = path.read_text(encoding="utf-8") if path.exists() else ""
            if expected_hash is not None and content_hash(current) != expected_hash:
                raise MemoryDocumentConflict(f"memory document changed concurrently: {name}")
            normalized = content.rstrip() + "\n"
            self._atomic_write(path, normalized)
            return MemoryDocumentSnapshot(
                name=name,
                content=normalized,
                content_hash=content_hash(normalized),
            )

    def _read_workspace_unlocked(self, scope_directory: str) -> MemoryWorkspaceSnapshot:
        directory = self._scope_path(scope_directory)
        documents: list[MemoryDocumentSnapshot] = []
        for name in MEMORY_DOCUMENT_NAMES:
            path = directory / name
            if not path.exists():
                raise FileNotFoundError(path)
            content = path.read_text(encoding="utf-8")
            documents.append(
                MemoryDocumentSnapshot(
                    name=name,
                    content=content,
                    content_hash=content_hash(content),
                )
            )
        return MemoryWorkspaceSnapshot(
            scope_directory=scope_directory,
            documents=tuple(documents),
        )

    def _scope_path(self, scope_directory: str) -> Path:
        if not scope_directory or scope_directory in {".", ".."}:
            raise ValueError("memory scope directory cannot be empty")
        if Path(scope_directory).name != scope_directory:
            raise ValueError("memory scope directory must be a single safe path component")
        return self._root / scope_directory

    def _lock(self, scope_directory: str) -> threading.RLock:
        with self._locks_guard:
            return self._locks.setdefault(scope_directory, threading.RLock())

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
