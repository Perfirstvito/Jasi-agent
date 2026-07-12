"""record query variants and lightweight reasoning model

Revision ID: 0012_memory_query_variants
Revises: 0011_akashic_model_routing
Create Date: 2026-07-12 11:30:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0012_memory_query_variants"
down_revision = "0011_akashic_model_routing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "memory_retrievals",
        sa.Column(
            "reasoning_model",
            sa.Text(),
            nullable=False,
            server_default="legacy-unrecorded",
        ),
    )
    op.add_column(
        "memory_retrievals",
        sa.Column(
            "query_variants",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.execute(
        """
        UPDATE memory_retrievals
        SET query_variants =
            jsonb_build_array(
                jsonb_build_object(
                    'kind', 'original',
                    'text', query,
                    'semantic', false,
                    'lexical', true,
                    'hit_record_ids', '[]'::jsonb
                )
            )
            || CASE WHEN rewritten_query IS NOT NULL THEN
                jsonb_build_array(
                    jsonb_build_object(
                        'kind', 'rewritten',
                        'text', rewritten_query,
                        'semantic', false,
                        'lexical', true,
                        'hit_record_ids', '[]'::jsonb
                    )
                )
            ELSE '[]'::jsonb END
            || CASE WHEN hyde_text IS NOT NULL THEN
                jsonb_build_array(
                    jsonb_build_object(
                        'kind', 'hyde',
                        'text', hyde_text,
                        'semantic', false,
                        'lexical', false,
                        'hit_record_ids', '[]'::jsonb
                    )
                )
            ELSE '[]'::jsonb END
        """
    )
    op.alter_column("memory_retrievals", "reasoning_model", server_default=None)


def downgrade() -> None:
    op.drop_column("memory_retrievals", "query_variants")
    op.drop_column("memory_retrievals", "reasoning_model")
