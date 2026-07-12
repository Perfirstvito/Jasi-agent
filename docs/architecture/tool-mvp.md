# Tool MVP Architecture

## Goal

The tool MVP gives every Runtime Profile one execution loop while keeping registration,
authorization, visibility, and implementation source separate. It deliberately does not add
MCP lifecycle management, plugin discovery, persistent tool preferences, or conversational
approval flows.

The core invariant is:

```text
visible tools <= authorized tools <= profile tools <= registered tools
```

Registration only means Jasi knows how to describe and execute a tool. It never grants model
access by itself.

## Built-in Catalog

Only these two tools are visible on the first model step:

- `get_current_time`
- `tool_search`

The remaining built-ins are registered but progressively disclosed:

| Capability | Tools |
| --- | --- |
| Public web | `web_search`, `web_fetch` |
| Workspace read | `read_file`, `list_dir` |
| Workspace mutation | `write_file`, `edit_file` |
| Command execution | `shell`, `task_output`, `task_stop` |
| Delivered conversation lookup | `search_messages`, `fetch_messages` |

Profile ceilings are explicit and immutable:

| Profile | Hidden capabilities it may unlock |
| --- | --- |
| `passive` | web, workspace read/write, commands, message lookup |
| `scheduled` | web, workspace read, message lookup |
| `proactive` | web |
| `drift` | web, message lookup |

All Profiles also include the two base tools. Write and command capabilities are therefore
limited to passive chat even before a Work grant narrows the ceiling further.

## Boundaries

`ToolRegistry` owns explicit registration, duplicate detection, JSON Schema validation,
argument validation, deterministic metadata search, result truncation, and handler execution.
Each `ToolSpec` records its source and search terms so a future MCP adapter can register remote
tools without changing Runtime.

Built-in handlers depend on narrow capabilities rather than Runtime. Message lookup receives a
`MessageLookupPort` and is forced to the current `conversation_id`. It returns user messages and
successfully delivered assistant messages only. Pending, failed, and `system_error` assistant
messages remain invisible, matching normal history semantics.

`FileWorkspace` resolves every file and command path under `JASI_TOOL_WORKSPACE`, including
symbolic links. Writes use atomic replacement and exact-match edits. `shell` never invokes Bash:
it executes one command from a fixed allowlist with a restricted environment, rejects operators,
redirects, traversal, and executable options, and binds background task access to the creating
session. `web_fetch` accepts public HTTP(S) targets only, revalidates every redirect, rejects URL
credentials and non-global DNS results, and caps downloaded and rendered content.
Clash-style fake-IP DNS is supported only through the explicit
`JASI_WEB_ALLOW_FAKE_IP_DNS=true` compatibility setting. Even then, literal reserved-IP URLs
remain blocked.

`RuntimeProfile.allowed_tools` is the chain-wide ceiling. `base_tools` is the safe subset shown
on the first model step. Runtime fails during assembly when a Profile refers to an unregistered
tool.

An agent Work may persist a narrower grant in its existing payload:

```json
{
  "tool_grant": {
    "tools": ["list_issues", "get_issue"]
  }
}
```

A grant can only select names already allowed by the Profile. Profile base tools remain
available because they are the chain's baseline. With no grant, the Profile ceiling remains
the effective authorization; creators of future scheduled and proactive tasks should persist a
grant whenever their Profile contains task-specific tools.

`ToolSession` is ephemeral Turn state. It starts with base tools visible, exposes immutable
snapshots to each model request, and accepts reveal requests only for authorized names. It does
not contain Channel, MCP, Skill, or persistence logic.

## Progressive Disclosure

`tool_search` is an ordinary registered tool. Its handler receives the effective authorized and
visible sets through `ToolExecutionContext`, searches only `authorized - visible`, and returns a
normal `ToolOutcome` with optional `reveal_tools`.

Runtime does not inspect the tool name or parse tool-specific JSON. It executes a complete model
tool-call batch against the visibility snapshot used for that model request, records every
result, and applies successful reveal requests after the batch. A model therefore cannot search
and execute a previously hidden tool in the same response. The next model step receives the new
Schema.

Any registered tool may return a reveal request, but `ToolSession` intersects it with effective
authorization. A tool cannot reveal a Profile-disallowed or Work-disallowed capability.

## Authorization Ownership

The MVP enforces a completed grant; it does not let the model approve its own capabilities. A
future application-layer planner may use an LLM to propose tool requirements, then combine the
proposal with Profile policy and user confirmation before writing the Work grant. Runtime will
not need to change for that flow.

Outbound delivery, Outbox retry, source acknowledgement, memory ingestion, and database commits
remain application concerns rather than model tools.

Akashic's `message_push` is intentionally excluded because it would bypass Work finalization,
channel policy, and the durable Outbox. Memory tools are excluded because Jasi's passive memory
pipeline owns consolidation and retrieval. Schedule tools, Skill loading, Spawn, vision, and MCP
lifecycle belong to their own application or adapter milestones rather than this common catalog.

## MCP And Skills

A future MCP adapter should use the official SDK, translate each remote operation into a narrow
`ToolSpec`, and register it before Runtime assembly. The model must not receive an `mcp_add` tool
that can start arbitrary processes.

Skills may explain when a tool is useful, but they cannot alter Profile or Work authorization.
