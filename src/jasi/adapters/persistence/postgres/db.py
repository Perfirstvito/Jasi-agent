from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Integer, Text
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

EXPECTED_ALEMBIC_REVISION = "0002_reliable_inbound"


class DatabaseNotReady(RuntimeError):
    pass


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = "conversations"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    external_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    meta: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )


class Message(Base):
    __tablename__ = "messages"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    origin: Mapped[str] = mapped_column(Text, nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    delivery_status: Mapped[str] = mapped_column(Text, nullable=False, server_default="sent")
    turn_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    meta: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )


class InboundEvent(Base):
    __tablename__ = "inbound_events"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    external_update_id: Mapped[str] = mapped_column(Text, nullable=False)
    conversation_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    external_user_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="processing")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Turn(Base):
    __tablename__ = "turns"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inbound_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    profile: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    step_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    usage: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ToolExecution(Base):
    __tablename__ = "tool_executions"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    turn_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tool_name: Mapped[str] = mapped_column(Text, nullable=False)
    arguments: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    result: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=sql_text("'{}'::jsonb")
    )
    risk: Mapped[str] = mapped_column(Text, nullable=False, server_default="low")
    status: Mapped[str] = mapped_column(Text, nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"
    __mapper_args__ = {"eager_defaults": True}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    external_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    segment_index: Mapped[int] = mapped_column(Integer, nullable=False)
    segment_count: Mapped[int] = mapped_column(Integer, nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    external_message_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=sql_text("now()")
    )


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def check_database_ready(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.execute(sql_text("SELECT 1"))
        version_table = await connection.scalar(
            sql_text("SELECT to_regclass('public.alembic_version')")
        )
        if version_table is None:
            raise DatabaseNotReady("database is reachable but Alembic migrations have not been run")
        current_revision = await connection.scalar(
            sql_text("SELECT version_num FROM alembic_version")
        )
        if current_revision != EXPECTED_ALEMBIC_REVISION:
            raise DatabaseNotReady(
                f"database revision is {current_revision!r}; expected {EXPECTED_ALEMBIC_REVISION!r}"
            )
