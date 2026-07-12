# Jasi

Jasi is a small durable agent runtime with Telegram passive chat, shared background Work,
and a passive long-term memory pipeline:

```text
Telegram private text
-> PassiveIngressService
-> durable Work
-> WorkWorker / AgentWorkHandler
-> AgentRuntime(passive profile)
   <- immutable TurnContextSnapshot (sent history + passive memory)
-> OpenAI-compatible Chat Completions and tools
-> WorkFinalizer
-> Outbox
-> Telegram reply
-> delivered-passive MemoryJob -> Markdown authority + PostgreSQL index
```

The first channel is intentionally narrow: private Telegram text only, no group chat,
no attachments, one main Runtime model, one lightweight background model, and one runtime
process. Runtime starts each Turn with `get_current_time` and `tool_search`; authorized built-in
tools are disclosed only when the model searches for the capability it needs.

Tools are registered explicitly. A Runtime Profile defines the chain-wide ceiling and its
base visible tools; an optional Work grant can only narrow non-base tools for one durable
task. `tool_search` searches only authorized hidden tools, and successful matches become
visible on the next model step. Runtime applies every reveal request generically and never
special-cases the search tool. See [Tool MVP Architecture](docs/architecture/tool-mvp.md).

The built-in catalog contains public web search/fetch, workspace-confined file read/list/write/
edit, sandboxed foreground/background Bash, delivered-message search/fetch, and read-only process
inspection. Passive chat may unlock the complete catalog. Scheduled, proactive, and drift
Profiles receive narrower read-only subsets.

`JASI_TOOL_WORKSPACE` is the only writable Shell mount. Bash supports pipelines, redirects,
subcommands, interpreters, and normal system tools inside Bubblewrap, while the repository,
`.env`, host filesystem, host processes, and sensitive environment variables remain unavailable.
Network is disabled by default and requires `JASI_SHELL_NETWORK_ENABLED=true`. Standard deletion
commands are rewritten into `.jasi-trash`; command policy remains an audit/rewrite layer, while
the OS sandbox is the actual security boundary. `list_processes` separately exposes process name,
PID, CPU, and memory for either Windows or the Jasi runtime without command lines or environment
variables. See [Shell Sandbox Architecture](docs/architecture/shell-sandbox.md).

`web_fetch` rejects non-public DNS targets. Networks using Clash-style fake-IP DNS may opt in
with `JASI_WEB_ALLOW_FAKE_IP_DNS=true`; this permits domain resolutions in `198.18.0.0/15` but
still rejects literal requests to that reserved range.

Jasi deliberately does not expose Akashic's `message_push` as a model tool. User-visible output
continues through Work finalization and the durable Outbox, so a tool call cannot bypass channel
policy or cause a second send. Memory mutation, scheduling, Skills, Spawn, vision, and MCP
lifecycle remain separate capabilities and are not part of this built-in migration.

Scheduled jobs are a separate timing domain. `at`, `interval`, and cron rules create a
unique occurrence and a durable Work in one transaction. A `direct` job skips the model;
an `agent` job uses the `scheduled` Runtime Profile. Both finish through the same Outbox.
Interval and cron misfires are coalesced to one occurrence after downtime. The current
MVP exposes schedule creation through `ScheduleService`; it does not yet add a Telegram
command or scheduling tool.

Proactive delivery is split into source ingestion and initiative planning. A registered
`SourcePort` is polled with a durable subscription cursor; source items are deduplicated
before `InitiativePlanner` creates a low-priority `proactive` Work. Per-session cooldown
is reserved in the same transaction as that Work. Source text is runtime input, not a
user chat message. The repository currently ships the connector boundary and workers,
but no concrete external feed connector is enabled by default.

External source acknowledgements and similar non-message side effects use a separate
Effect Outbox. A source poll commits its cursor, items, and effect records atomically;
EffectWorker executes them later with deduplication and retry. Effect adapters are also
explicitly registered and none are enabled by default.

`OperationsService.snapshot()` provides aggregate queue counts, due trigger counts, and
expired Work lease counts for health checks. It intentionally returns no prompts,
message text, source payloads, credentials, or other sensitive records.

Drift uses the same InitiativePlanner and AgentWorkHandler with a different candidate
source and Runtime Profile. `DriftOpportunityProducer` persists ideas from future
source/Skill/Memory producers. The planner waits for the configured idle window and
cooldown before creating priority-20 Work. A new passive message atomically cancels any
pending drift Work for that session, so a stale conversation opener cannot follow a
fresh user message.

## Passive Memory

Passive memory has one content authority: four Markdown files under
`JASI_MEMORY_ROOT/<scope-directory>/`:

- `MEMORY.md` contains stable profile facts. Human-authored text is preserved; Jasi only
  rewrites its marked managed section.
- `HISTORY.md` is append-only for automatically extracted episodic memories.
- `RECENT_CONTEXT.md` summarizes delivered messages that have moved outside the raw
  30-message history window.
- `PENDING.md` exposes extracted candidates while consolidation is in progress and is
  cleared only after the authoritative files have been updated.

PostgreSQL stores rebuildable parsed records, optional pgvector embeddings, evidence links,
retrieval audits, checkpoints, and leased MemoryJobs. It never writes content back into
Markdown. Persist `JASI_MEMORY_ROOT`; losing that directory is data loss by design, even if
the database index still exists.

A consolidation job is created in the same transaction that marks the final Outbox part of
a successful passive model reply as sent. Jobs are claimed in batches targeting 4-8 passive
messages (6 by default). Failed, pending, and system-error assistant messages never enter the
window. A sent proactive message may provide context only when a later passive batch spans it;
it does not create a MemoryJob and cannot be the evidence for a user fact.

Stable Markdown and recent summaries are always supplied as derived reference context.
Episodic recall preserves the exact user utterance as the anchor query, then adds rewritten
and optional HyDE queries. Original and rewritten text both use semantic and trigram search;
the audit records every variant and its hit IDs before merge, rerank, sufficiency, and context
budgeting. This prevents a concise rewrite from silently dropping dates, names, punctuation,
or other user details.

`JASI_OPENAI_*` is reserved for user-visible Runtime execution. Memory extraction,
reconciliation, summaries, Gate, Rewrite, HyDE, Rerank, and Sufficiency use the independent
`JASI_LIGHT_MODEL_*` endpoint. If all three light endpoint values are omitted, Jasi falls back
to the main model for compatibility.

Embedding is optional. Leave `JASI_MEMORY_EMBEDDING_BASE_URL` and
`JASI_MEMORY_EMBEDDING_API_KEY` unset for lexical-only retrieval. Configure a real
OpenAI-compatible embeddings endpoint to enable 1024-dimensional hybrid retrieval. The checked
example matches Akashic's DashScope `text-embedding-v3` configuration; many chat-only providers
do not implement this API.

By default, memory identity is `channel:user_id`. `JASI_MEMORY_SCOPE_MAP` can map Telegram,
Feishu, or future channel identities to one owner scope:

```dotenv
JASI_MEMORY_SCOPE_MAP={"telegram:123456789":"owner","feishu:ou_xxx":"owner"}
```

The mapping is resolved before Runtime and does not add channel logic to memory or the model
loop. See [Passive Memory Architecture](docs/architecture/passive-memory.md) for recovery and
ownership details.

## Boundaries and Recovery

- `AgentRuntime` only depends on `RuntimeRepositoryPort` for history, Turns, and tool
  records, plus a `TurnContextProviderPort` that returns an immutable snapshot. It never
  imports channel, Outbox, Markdown, embedding, or SQLAlchemy code.
- `PassiveIngressService` only commits the inbound event, user message, and passive Work
  atomically. Telegram can acknowledge an update as soon as that transaction succeeds.
- `AgentWorkHandler` converts persisted Work into a typed runtime request. It has no
  Telegram-specific behavior.
- `WorkFinalizer` applies the target channel's outbound policy, then atomically commits
  the completed Work, assistant message, Outbox parts, and inbound state.
- `OutboxWorker` depends on `OutboxRepositoryPort` and dispatches each record through
  the sender registered for that channel.
- `MemoryWorker` starts only after a passive reply is fully delivered. It owns extraction,
  Markdown maintenance, reindexing, leases, retry, and checkpoints outside Runtime.
- `ScheduleWorker` only turns due PostgreSQL jobs into occurrence-linked Work. It never
  calls Runtime or a Channel.
- `SourceWorker` persists cursor and source items; `InitiativePlanner` converts eligible
  candidates to Work. Neither owns a model loop or sends directly.
- PostgreSQL uses one repository implementation for these three narrow ports; callers
  only receive the capability they need.

An inbound event becomes durable before model execution. Completed runtime results are
checkpointed by Work ID on the Turn, so replay after a finalization failure does not call
the model again. PostgreSQL leases serialize all Work for one session while allowing
different sessions to execute concurrently. Telegram advances its polling offset once
the fetched updates have been durably enqueued.

## Run Locally

1. Install [uv](https://docs.astral.sh/uv/) if it is not already available.
2. Install the pinned Python version and all dependencies:

   ```bash
   uv python install
   uv sync
   ```

   `uv sync` creates `.venv`, installs Jasi in editable mode, and installs the
   development dependency group from the checked-in `uv.lock` file.

3. Start PostgreSQL:

   ```bash
   docker compose up -d
   ```

4. Export environment variables from `.env.example` after filling in secrets:

   ```bash
   set -a
   . ./.env
   set +a
   ```

   Remove the optional `JASI_MEMORY_EMBEDDING_*` values unless they point to a working
   embeddings API. Jasi otherwise runs in lexical-only mode.

5. Run migrations explicitly:

   ```bash
   uv run alembic upgrade head
   ```

6. Start the bot:

   ```bash
   uv run python -m jasi
   ```

Startup checks the database connection and Alembic revision. It exits with a clear
error if the migration is missing or stale.

Sandboxed Shell requires `bubblewrap` and `prlimit` on Linux/WSL. If either is unavailable, Jasi
continues running but Shell calls are rejected instead of falling back to unsandboxed execution.

## Telegram Bot Setup

Create a bot with BotFather, set `JASI_TELEGRAM_BOT_TOKEN`, and configure
`JASI_TELEGRAM_ALLOWED_USER_IDS` with comma-separated numeric Telegram user IDs.
An empty allowlist is rejected at startup. `JASI_TELEGRAM_MAX_CONCURRENCY` bounds inbound
database work; `JASI_WORK_BATCH_SIZE` bounds agent execution. Messages in one chat are
serialized by the durable Work queue.

## Tests

Unit tests use fakes for the model, channel, and repository:

```bash
uv run pytest
```

Integration tests that require real PostgreSQL are marked with `integration` and
can be run once the Compose database is up and migrated by setting
`JASI_TEST_DATABASE_URL` to a disposable test database URL:

```bash
JASI_TEST_DATABASE_URL=postgresql+asyncpg://jasi:jasi@localhost:5432/jasi_test \
  uv run pytest -q tests/integration
```

The migration integration test creates and drops a temporary database, so the configured
PostgreSQL user needs `CREATEDB`. Never point this variable at production.
