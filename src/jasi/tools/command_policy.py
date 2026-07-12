from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol

import bashlex
from bashlex.errors import ParsingError

CommandRisk = Literal["read-only", "write", "network", "destructive"]

_BLOCKED_COMMANDS = frozenset(
    {
        "bwrap",
        "chroot",
        "doas",
        "mount",
        "nsenter",
        "pivot_root",
        "su",
        "sudo",
        "umount",
        "unshare",
    }
)
_SOFT_DELETE_COMMANDS = frozenset({"rm", "rmdir", "shred", "unlink"})
_NETWORK_COMMANDS = frozenset(
    {
        "aria2c",
        "curl",
        "ftp",
        "git",
        "http",
        "httpie",
        "nc",
        "npm",
        "pip",
        "pip3",
        "scp",
        "ssh",
        "wget",
        "xh",
    }
)
_WRITE_COMMANDS = frozenset(
    {
        "chmod",
        "chown",
        "cp",
        "dd",
        "install",
        "ln",
        "mkdir",
        "mv",
        "patch",
        "sed",
        "tee",
        "touch",
        "truncate",
    }
)
_WRAPPER_COMMANDS = frozenset({"command", "exec", "nohup"})
_NESTED_SHELLS = frozenset({"bash", "dash", "fish", "sh", "zsh"})
_TRASH_COMMAND = "/usr/bin/python3 /jasi/bin/jasi_trash.py"


class CommandPolicyRejected(ValueError):
    pass


@dataclass(frozen=True)
class CommandRewrite:
    kind: str
    original: str
    replacement: str
    reversible: bool


@dataclass(frozen=True)
class CommandPolicyState:
    original_command: str
    command: str
    risk: CommandRisk = "read-only"
    rewrites: tuple[CommandRewrite, ...] = ()


class CommandPolicyHook(Protocol):
    def apply(self, state: CommandPolicyState) -> CommandPolicyState: ...


class CommandPolicyPipeline:
    def __init__(self, hooks: tuple[CommandPolicyHook, ...]) -> None:
        if not hooks:
            raise ValueError("command policy requires at least one hook")
        self._hooks = hooks

    def evaluate(self, command: str) -> CommandPolicyState:
        normalized = command.strip()
        if not normalized:
            raise CommandPolicyRejected("command cannot be empty")
        state = CommandPolicyState(original_command=normalized, command=normalized)
        for hook in self._hooks:
            state = hook.apply(state)
            if state.original_command != normalized:
                raise TypeError("command policy hook changed the original command")
        return state


class BashSyntaxGuard:
    def apply(self, state: CommandPolicyState) -> CommandPolicyState:
        nodes = _parse(state.command)
        for command_node in _command_nodes(nodes):
            word = _effective_command_word(command_node)
            if word is None:
                continue
            if getattr(word, "parts", ()):
                raise CommandPolicyRejected("dynamic command names are not allowed")
            executable = PurePosixPath(str(word.word)).name.casefold()
            if executable in _BLOCKED_COMMANDS or str(word.word) in {".", "eval", "source"}:
                raise CommandPolicyRejected(f"command is blocked inside the sandbox: {executable}")
            direct_words = _direct_words(command_node)
            if executable in _NESTED_SHELLS and any(
                str(item.word) in {"-c", "-lc"} for item in direct_words[1:]
            ):
                raise CommandPolicyRejected(
                    "nested shell command strings are blocked; run the Bash expression directly"
                )
            if executable == "xargs" and any(
                PurePosixPath(str(item.word)).name.casefold() in _SOFT_DELETE_COMMANDS
                for item in direct_words[1:]
            ):
                raise CommandPolicyRejected(
                    "xargs deletion is blocked; use rm for reversible deletion"
                )
            if executable == "find" and any(
                str(item.word) in {"-delete", "-exec", "-execdir", "-ok", "-okdir"}
                for item in direct_words[1:]
            ):
                raise CommandPolicyRejected(
                    "find deletion and execution actions are blocked; "
                    "use rm for reversible deletion"
                )
            if executable == "git" and any(str(item.word) == "clean" for item in direct_words[1:3]):
                raise CommandPolicyRejected("git clean is blocked; use rm for reversible deletion")
        return state


class SoftDeleteTransform:
    def apply(self, state: CommandPolicyState) -> CommandPolicyState:
        replacements: list[tuple[int, int, str, str]] = []
        for command_node in _command_nodes(_parse(state.command)):
            word = _effective_command_word(command_node)
            if word is None or getattr(word, "parts", ()):
                continue
            executable = PurePosixPath(str(word.word)).name.casefold()
            if executable in _SOFT_DELETE_COMMANDS:
                start, end = word.pos
                replacements.append((start, end, state.command[start:end], _TRASH_COMMAND))
        if not replacements:
            return state

        rewritten = state.command
        records: list[CommandRewrite] = []
        for start, end, original, replacement_text in sorted(replacements, reverse=True):
            rewritten = rewritten[:start] + replacement_text + rewritten[end:]
            records.append(
                CommandRewrite(
                    kind="soft_delete",
                    original=original,
                    replacement=replacement_text,
                    reversible=True,
                )
            )
        return replace(
            state,
            command=rewritten,
            risk="destructive",
            rewrites=state.rewrites + tuple(reversed(records)),
        )


class CommandRiskClassifier:
    def apply(self, state: CommandPolicyState) -> CommandPolicyState:
        if state.risk == "destructive":
            return state
        risk: CommandRisk = "read-only"
        nodes = _parse(state.command)
        for node in _walk_nodes(nodes):
            if getattr(node, "kind", None) == "redirect":
                risk = _higher_risk(risk, "write")
        for command_node in _command_nodes(nodes):
            word = _effective_command_word(command_node)
            if word is None or getattr(word, "parts", ()):
                continue
            executable = PurePosixPath(str(word.word)).name.casefold()
            if executable in _NETWORK_COMMANDS:
                risk = _higher_risk(risk, "network")
            elif executable in _WRITE_COMMANDS:
                risk = _higher_risk(risk, "write")
        return replace(state, risk=risk)


def build_default_command_policy() -> CommandPolicyPipeline:
    return CommandPolicyPipeline(
        (
            BashSyntaxGuard(),
            SoftDeleteTransform(),
            BashSyntaxGuard(),
            CommandRiskClassifier(),
        )
    )


def _parse(command: str) -> list[Any]:
    try:
        return list(bashlex.parse(command))
    except (ParsingError, ValueError, NotImplementedError) as exc:
        raise CommandPolicyRejected(f"unsupported or invalid Bash syntax: {exc}") from exc


def _walk_nodes(nodes: list[Any]) -> list[Any]:
    found: list[Any] = []
    pending = list(reversed(nodes))
    seen: set[int] = set()
    while pending:
        node = pending.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        found.append(node)
        for value in vars(node).values():
            if hasattr(value, "kind"):
                pending.append(value)
            elif isinstance(value, list):
                pending.extend(reversed([item for item in value if hasattr(item, "kind")]))
    return found


def _command_nodes(nodes: list[Any]) -> list[Any]:
    return [node for node in _walk_nodes(nodes) if getattr(node, "kind", None) == "command"]


def _direct_words(command_node: Any) -> list[Any]:
    return [
        part for part in getattr(command_node, "parts", ()) if getattr(part, "kind", None) == "word"
    ]


def _effective_command_word(command_node: Any) -> Any | None:
    words = _direct_words(command_node)
    if not words:
        return None
    executable = PurePosixPath(str(words[0].word)).name.casefold()
    if executable == "command" and any(str(word.word) in {"-v", "-V"} for word in words[1:]):
        return words[0]
    if executable in _WRAPPER_COMMANDS:
        return next((word for word in words[1:] if not str(word.word).startswith("-")), words[0])
    if executable == "env":
        for word in words[1:]:
            value = str(word.word)
            if value.startswith("-") or "=" in value:
                continue
            return word
    return words[0]


def _higher_risk(current: CommandRisk, candidate: CommandRisk) -> CommandRisk:
    order = {"read-only": 0, "write": 1, "network": 2, "destructive": 3}
    return candidate if order[candidate] > order[current] else current
