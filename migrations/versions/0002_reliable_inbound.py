"""make inbound processing recoverable

Revision ID: 0002_reliable_inbound
Revises: 0001_initial
Create Date: 2026-07-11 17:15:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_reliable_inbound"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "inbound_events",
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'processing'"),
        ),
    )
    op.add_column(
        "inbound_events",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.add_column("inbound_events", sa.Column("last_error", sa.Text(), nullable=True))
    op.add_column(
        "inbound_events",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.add_column(
        "inbound_events",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_inbound_events_status",
        "inbound_events",
        "status IN ('pending', 'processing', 'completed')",
    )

    op.add_column("turns", sa.Column("final_text", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE turns AS turn_row
        SET final_text = message_row.content
        FROM messages AS message_row
        WHERE message_row.turn_id = turn_row.id
          AND message_row.role = 'assistant'
        """
    )
    op.create_unique_constraint(
        "uq_turns_inbound_message_id",
        "turns",
        ["inbound_message_id"],
    )

    op.execute(
        """
        UPDATE inbound_events AS event_row
        SET status = 'completed',
            completed_at = now(),
            updated_at = now()
        WHERE EXISTS (
            SELECT 1
            FROM turns AS turn_row
            JOIN messages AS message_row ON message_row.turn_id = turn_row.id
            WHERE turn_row.inbound_message_id = event_row.message_id
              AND message_row.role = 'assistant'
        )
        """
    )


def downgrade() -> None:
    op.drop_constraint("uq_turns_inbound_message_id", "turns", type_="unique")
    op.drop_column("turns", "final_text")
    op.drop_constraint("ck_inbound_events_status", "inbound_events", type_="check")
    op.drop_column("inbound_events", "completed_at")
    op.drop_column("inbound_events", "updated_at")
    op.drop_column("inbound_events", "last_error")
    op.drop_column("inbound_events", "attempts")
    op.drop_column("inbound_events", "status")
