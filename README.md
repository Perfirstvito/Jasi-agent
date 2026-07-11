# Jasi

Jasi is a minimal Telegram passive-chat MVP:

```text
Telegram private text
-> PassiveChatService
-> AgentRuntime(passive profile)
-> OpenAI-compatible Chat Completions and tools
-> PostgreSQL persistence
-> Outbox
-> Telegram reply
```

The first profile is intentionally narrow: private Telegram text only, no group chat,
no attachments, one configured model, one runtime process, and one tool
(`get_current_time`).

## Boundaries and Recovery

- `AgentRuntime` only depends on `RuntimeRepositoryPort` for history, Turns, and tool
  records. It never imports channel or Outbox code.
- `PassiveChatService` depends on `ChatRepositoryPort`. It maps a runtime result through
  the channel's outbound policy and commits the assistant message, Outbox parts, and
  completed inbound state atomically.
- `OutboxWorker` depends on `OutboxRepositoryPort` and dispatches each record through
  the sender registered for that channel.
- PostgreSQL uses one repository implementation for these three narrow ports; callers
  only receive the capability they need.

An inbound event remains recoverable until its Outbox records are committed. Completed
runtime results are checkpointed on the Turn, so replay after a response-transaction
failure does not call the model again. Telegram advances its polling offset only after
the entire fetched batch has been handled successfully.

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
An empty allowlist is rejected at startup. `JASI_TELEGRAM_MAX_CONCURRENCY` bounds work
across chats; messages in the same chat remain serialized by the service.

## Tests

Unit tests use fakes for the model, channel, and repository:

```bash
uv run pytest
```

Integration tests that require real PostgreSQL are marked with `integration` and
can be run once the Compose database is up and migrated by setting
`JASI_TEST_DATABASE_URL` to a test database URL.
