# Jasi

Jasi is a small durable agent runtime with Telegram passive chat and scheduled delivery:

```text
Telegram private text
-> PassiveIngressService
-> durable Work
-> WorkWorker / AgentWorkHandler
-> AgentRuntime(passive profile)
-> OpenAI-compatible Chat Completions and tools
-> WorkFinalizer
-> Outbox
-> Telegram reply
```

The first profile is intentionally narrow: private Telegram text only, no group chat,
no attachments, one configured model, one runtime process, and one tool
(`get_current_time`).

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

## Boundaries and Recovery

- `AgentRuntime` only depends on `RuntimeRepositoryPort` for history, Turns, and tool
  records. It never imports channel or Outbox code.
- `PassiveIngressService` only commits the inbound event, user message, and passive Work
  atomically. Telegram can acknowledge an update as soon as that transaction succeeds.
- `AgentWorkHandler` converts persisted Work into a typed runtime request. It has no
  Telegram-specific behavior.
- `WorkFinalizer` applies the target channel's outbound policy, then atomically commits
  the completed Work, assistant message, Outbox parts, and inbound state.
- `OutboxWorker` depends on `OutboxRepositoryPort` and dispatches each record through
  the sender registered for that channel.
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
`JASI_TEST_DATABASE_URL` to a test database URL.
