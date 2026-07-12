# Shell Sandbox Architecture

## Goal

Jasi gives passive chat useful Bash semantics without giving model-authored commands ambient
access to the Jasi repository, credentials, host filesystem, network, or host processes.

The key distinction is:

```text
command policy = parse, classify, reject, rewrite, audit
OS sandbox      = containment
```

A blacklist or LLM review is never considered a containment boundary.

## Execution

The Shell handler sends a typed command request through explicit policy hooks:

1. Parse common Bash syntax and reject dynamic command names or sandbox-control commands.
2. Reject deletion forms that cannot be made reversible, such as `find -delete` and `git clean`.
3. Rewrite `rm`, `rmdir`, `unlink`, and `shred`, including nested command substitutions, to the
   read-only `jasi_trash.py` helper.
4. Classify the resulting command as read-only, write, network, or destructive.

The helper moves targets to `JASI_TOOL_WORKSPACE/.jasi-trash/<batch>/<original-path>`. This makes
standard deletion recoverable. It is not a claim that arbitrary programs cannot overwrite data;
the configured tool workspace remains an agent-controlled writable area and must not contain the
only copy of irreplaceable data.

`CommandTaskManager` then launches `prlimit -> bwrap -> bash` without an intermediate host shell.
Foreground cancellation, timeout, background stop, and application shutdown kill the complete
process group. Background IDs are random and can only be read or stopped by their creating
session.

## Sandbox Boundary

The Bubblewrap process receives:

- `/usr` read-only, with standard `/bin`, `/lib`, `/lib64`, and `/sbin` links;
- a private `/proc`, `/dev`, `/tmp`, `/home`, and hostname;
- the configured tool workspace mounted read/write at `/workspace`;
- the soft-delete helper mounted read-only;
- an empty environment rebuilt with only `HOME`, `PATH`, locale, and `JASI_WORKSPACE`.

It does not receive the repository root, `.env`, user home, `/mnt/c`, WSL interop socket, or other
host mounts. All capabilities are dropped. Address space, output-file size, open files, process
count, CPU time, wall time, and captured output are bounded.

Network has its own namespace by default. Setting `JASI_SHELL_NETWORK_ENABLED=true` retains the
host network namespace and mounts only the DNS and CA files needed by clients. This is an explicit
operator decision because a networked Shell can transmit workspace content.

## Process Inspection

Host process inspection is intentionally not achieved by weakening the Shell sandbox.
`list_processes` uses a fixed adapter command:

- `scope=runtime`: fixed `ps` fields for the Linux/WSL runtime;
- `scope=windows`: fixed non-interactive PowerShell `Get-Process` projection;
- `scope=auto`: Windows when available, otherwise runtime.

Filtering and sorting happen in Python. Results contain only PID, process name, accumulated CPU,
and working-set memory. Command lines and environments are never requested.

## Degradation

Bubblewrap Shell is available on Linux/WSL only. Missing `bwrap`, `prlimit`, Bash, or the helper
causes a clear tool rejection; Jasi does not fall back to raw host execution. Process inspection
advertises only scopes supported by the current OS. Runtime, Profiles, progressive disclosure,
Work, and Outbox behavior do not change.
