from __future__ import annotations

import pytest

from jasi.tools.command_policy import CommandPolicyRejected, build_default_command_policy


def test_policy_accepts_general_bash_and_classifies_side_effects() -> None:
    policy = build_default_command_policy()

    read = policy.evaluate("ps aux | head -5 && printf '%s\\n' done")
    write = policy.evaluate("printf data > result.txt && python3 -c 'print(1)'")
    network = policy.evaluate("curl https://example.com | head")

    assert read.risk == "read-only"
    assert write.risk == "write"
    assert network.risk == "network"
    assert read.rewrites == ()


def test_policy_rewrites_standard_deletion_commands_at_every_ast_depth() -> None:
    decision = build_default_command_policy().evaluate(
        "rm -rf one; value=$(/bin/unlink two); command rmdir three; env A=1 shred four"
    )

    assert decision.risk == "destructive"
    assert len(decision.rewrites) == 4
    assert all(rewrite.kind == "soft_delete" for rewrite in decision.rewrites)
    assert all(rewrite.reversible for rewrite in decision.rewrites)
    assert " rm " not in f" {decision.command} "
    assert "/bin/unlink" not in decision.command
    assert decision.command.count("/jasi/bin/jasi_trash.py") == 4


def test_policy_does_not_rewrite_command_introspection() -> None:
    decision = build_default_command_policy().evaluate("command -v rm")

    assert decision.command == "command -v rm"
    assert decision.rewrites == ()


@pytest.mark.parametrize(
    "command",
    [
        "sudo id",
        "bwrap --ro-bind / / true",
        "find . -delete",
        "find . -exec rm {} +",
        "git clean -fdx",
        "xargs rm < files.txt",
        "bash -c 'rm victim'",
        "eval 'rm victim'",
        '"$COMMAND" --version',
        "echo 'unterminated",
    ],
)
def test_policy_rejects_escape_and_uninspectable_dispatch(command: str) -> None:
    with pytest.raises(CommandPolicyRejected):
        build_default_command_policy().evaluate(command)
