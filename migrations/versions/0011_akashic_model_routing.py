"""align memory vectors with the Akashic embedding model

Revision ID: 0011_akashic_model_routing
Revises: 0010_memory_job_batching
Create Date: 2026-07-12 11:00:00.000000
"""

from __future__ import annotations

from alembic import op
from pgvector.sqlalchemy import Vector

revision = "0011_akashic_model_routing"
down_revision = "0010_memory_job_batching"
branch_labels = None
depends_on = None

OLD_DIMENSIONS = 1536
NEW_DIMENSIONS = 1024


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_memory_records_embedding_hnsw")
    # Vectors from another model space cannot be reused safely. Markdown remains authoritative
    # and the normal reindex worker rebuilds every vector with the configured model.
    op.execute("UPDATE memory_records SET embedding = NULL, embedding_model = NULL")
    op.execute(
        "UPDATE memory_documents SET indexed_hash = '' WHERE name IN ('MEMORY.md', 'HISTORY.md')"
    )
    op.alter_column(
        "memory_records",
        "embedding",
        existing_type=Vector(OLD_DIMENSIONS),
        type_=Vector(NEW_DIMENSIONS),
        postgresql_using=f"NULL::vector({NEW_DIMENSIONS})",
    )
    op.execute(
        "CREATE INDEX ix_memory_records_embedding_hnsw ON memory_records "
        "USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_memory_records_embedding_hnsw")
    op.execute("UPDATE memory_records SET embedding = NULL, embedding_model = NULL")
    op.execute(
        "UPDATE memory_documents SET indexed_hash = '' WHERE name IN ('MEMORY.md', 'HISTORY.md')"
    )
    op.alter_column(
        "memory_records",
        "embedding",
        existing_type=Vector(NEW_DIMENSIONS),
        type_=Vector(OLD_DIMENSIONS),
        postgresql_using=f"NULL::vector({OLD_DIMENSIONS})",
    )
    op.execute(
        "CREATE INDEX ix_memory_records_embedding_hnsw ON memory_records "
        "USING hnsw (embedding vector_cosine_ops)"
    )
