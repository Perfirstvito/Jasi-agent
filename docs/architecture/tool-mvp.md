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

## Boundaries

`ToolRegistry` owns explicit registration, duplicate detection, JSON Schema validation,
argument validation, deterministic metadata search, result truncation, and handler execution.
Each `ToolSpec` records its source and search terms so a future MCP adapter can register remote
tools without changing Runtime.

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

## MCP And Skills

A future MCP adapter should use the official SDK, translate each remote operation into a narrow
`ToolSpec`, and register it before Runtime assembly. The model must not receive an `mcp_add` tool
that can start arbitrary processes.

Skills may explain when a tool is useful, but they cannot alter Profile or Work authorization.
