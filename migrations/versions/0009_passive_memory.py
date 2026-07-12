"""add passive memory scopes, index, jobs, and retrieval audit

Revision ID: 0009_passive_memory
Revises: 0008_effect_outbox
Create Date: 2026-07-12 09:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

revision = "0009_passive_memory"
down_revision = "0008_effect_outbox"
branch_labels = None
depends_on = None

EMBEDDING_DIMENSIONS = 1536


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.create_table(
        "memory_scopes",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("scope_key", sa.Text(), nullable=False),
        sa.Column("directory_name", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("btrim(scope_key) <> ''", name="ck_memory_scopes_key"),
        sa.CheckConstraint("btrim(directory_name) <> ''", name="ck_memory_scopes_directory"),
        sa.UniqueConstraint("scope_key", name="uq_memory_scopes_key"),
        sa.UniqueConstraint("directory_name", name="uq_memory_scopes_directory"),
    )

    op.create_table(
        "conversation_memory_scopes",
        sa.Column("conversation_id", sa.BigInteger(), primary_key=True),
        sa.Column("scope_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_conversation_memory_scopes_conversation",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["scope_id"],
            ["memory_scopes.id"],
            name="fk_conversation_memory_scopes_scope",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_conversation_memory_scopes_scope",
        "conversation_memory_scopes",
        ["scope_id"],
    )

    op.create_table(
        "memory_documents",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("scope_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False, server_default=""),
        sa.Column("indexed_hash", sa.Text(), nullable=False, server_default=""),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "name IN ('MEMORY.md', 'HISTORY.md', 'RECENT_CONTEXT.md', 'PENDING.md')",
            name="ck_memory_documents_name",
        ),
        sa.CheckConstraint("version >= 0", name="ck_memory_documents_version"),
        sa.ForeignKeyConstraint(
            ["scope_id"],
            ["memory_scopes.id"],
            name="fk_memory_documents_scope",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("scope_id", "name", name="uq_memory_documents_scope_name"),
    )

    op.create_table(
        "memory_records",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("scope_id", sa.BigInteger(), nullable=False),
        sa.Column("document_id", sa.BigInteger(), nullable=False),
        sa.Column("record_key", sa.Text(), nullable=False),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="active"),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("heading", sa.Text(), nullable=False, server_default=""),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column(
            "tags",
            postgresql.ARRAY(sa.Text()),
            nullable=False,
            server_default=sa.text("ARRAY[]::text[]"),
        ),
        sa.Column("embedding", Vector(EMBEDDING_DIMENSIONS), nullable=True),
        sa.Column("embedding_model", sa.Text(), nullable=True),
        sa.Column("happened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("tier IN ('stable', 'episodic')", name="ck_memory_records_tier"),
        sa.CheckConstraint(
            "status IN ('active', 'stale')",
            name="ck_memory_records_status",
        ),
        sa.CheckConstraint("ordinal >= 0", name="ck_memory_records_ordinal"),
        sa.CheckConstraint("btrim(content) <> ''", name="ck_memory_records_content"),
        sa.ForeignKeyConstraint(
            ["scope_id"],
            ["memory_scopes.id"],
            name="fk_memory_records_scope",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["document_id"],
            ["memory_documents.id"],
            name="fk_memory_records_document",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("scope_id", "record_key", name="uq_memory_records_scope_key"),
    )
    op.create_index(
        "ix_memory_records_scope_tier_status",
        "memory_records",
        ["scope_id", "tier", "status"],
    )
    op.execute(
        "CREATE INDEX ix_memory_records_embedding_hnsw ON memory_records "
        "USING hnsw (embedding vector_cosine_ops)"
    )
    op.execute(
        "CREATE INDEX ix_memory_records_content_trgm ON memory_records "
        "USING gin (content gin_trgm_ops)"
    )

    op.create_table(
        "memory_evidence",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("record_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["record_id"],
            ["memory_records.id"],
            name="fk_memory_evidence_record",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["message_id"],
            ["messages.id"],
            name="fk_memory_evidence_message",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("record_id", "message_id", name="uq_memory_evidence_record_message"),
    )
    op.create_index("ix_memory_evidence_message", "memory_evidence", ["message_id"])

    op.create_table(
        "memory_jobs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("scope_id", sa.BigInteger(), nullable=False),
        sa.Column("conversation_id", sa.BigInteger(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("trigger_message_id", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "kind IN ('consolidate', 'reindex')",
            name="ck_memory_jobs_kind",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'failed')",
            name="ck_memory_jobs_status",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_memory_jobs_attempts"),
        sa.CheckConstraint("max_attempts > 0", name="ck_memory_jobs_max_attempts"),
        sa.ForeignKeyConstraint(
            ["scope_id"],
            ["memory_scopes.id"],
            name="fk_memory_jobs_scope",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_memory_jobs_conversation",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["trigger_message_id"],
            ["messages.id"],
            name="fk_memory_jobs_trigger_message",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("dedupe_key", name="uq_memory_jobs_dedupe_key"),
    )
    op.create_index(
        "ix_memory_jobs_ready",
        "memory_jobs",
        ["status", "available_at", "created_at"],
    )
    op.create_index(
        "ix_memory_jobs_scope_status",
        "memory_jobs",
        ["scope_id", "status", "kind"],
    )
    op.create_index(
        "uq_memory_jobs_running_scope",
        "memory_jobs",
        ["scope_id"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )

    op.create_table(
        "memory_checkpoints",
        sa.Column("scope_id", sa.BigInteger(), primary_key=True),
        sa.Column("conversation_id", sa.BigInteger(), primary_key=True),
        sa.Column(
            "consolidated_through_sequence",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "summarized_through_sequence",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "consolidated_through_sequence >= 0",
            name="ck_memory_checkpoints_consolidated",
        ),
        sa.CheckConstraint(
            "summarized_through_sequence >= 0",
            name="ck_memory_checkpoints_summarized",
        ),
        sa.ForeignKeyConstraint(
            ["scope_id"],
            ["memory_scopes.id"],
            name="fk_memory_checkpoints_scope",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_memory_checkpoints_conversation",
            ondelete="CASCADE",
        ),
    )

    op.create_table(
        "memory_retrievals",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("turn_id", sa.BigInteger(), nullable=False),
        sa.Column("scope_id", sa.BigInteger(), nullable=False),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("rewritten_query", sa.Text(), nullable=True),
        sa.Column("hyde_text", sa.Text(), nullable=True),
        sa.Column("gate_decision", sa.Text(), nullable=False),
        sa.Column("sufficient", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column(
            "trace",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "gate_decision IN ('retrieve', 'skip', 'fallback')",
            name="ck_memory_retrievals_gate",
        ),
        sa.ForeignKeyConstraint(
            ["turn_id"],
            ["turns.id"],
            name="fk_memory_retrievals_turn",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["scope_id"],
            ["memory_scopes.id"],
            name="fk_memory_retrievals_scope",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("turn_id", name="uq_memory_retrievals_turn"),
    )

    op.create_table(
        "memory_retrieval_hits",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("retrieval_id", sa.BigInteger(), nullable=False),
        sa.Column("record_id", sa.BigInteger(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("semantic_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("lexical_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("rerank_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("final_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("injected", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("content_snapshot", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["retrieval_id"],
            ["memory_retrievals.id"],
            name="fk_memory_retrieval_hits_retrieval",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["record_id"],
            ["memory_records.id"],
            name="fk_memory_retrieval_hits_record",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("retrieval_id", "rank", name="uq_memory_retrieval_hits_rank"),
    )


def downgrade() -> None:
    op.drop_table("memory_retrieval_hits")
    op.drop_table("memory_retrievals")
    op.drop_table("memory_checkpoints")
    op.drop_index("uq_memory_jobs_running_scope", table_name="memory_jobs")
    op.drop_index("ix_memory_jobs_scope_status", table_name="memory_jobs")
    op.drop_index("ix_memory_jobs_ready", table_name="memory_jobs")
    op.drop_table("memory_jobs")
    op.drop_index("ix_memory_evidence_message", table_name="memory_evidence")
    op.drop_table("memory_evidence")
    op.execute("DROP INDEX IF EXISTS ix_memory_records_content_trgm")
    op.execute("DROP INDEX IF EXISTS ix_memory_records_embedding_hnsw")
    op.drop_index("ix_memory_records_scope_tier_status", table_name="memory_records")
    op.drop_table("memory_records")
    op.drop_table("memory_documents")
    op.drop_index(
        "ix_conversation_memory_scopes_scope",
        table_name="conversation_memory_scopes",
    )
    op.drop_table("conversation_memory_scopes")
    op.drop_table("memory_scopes")
