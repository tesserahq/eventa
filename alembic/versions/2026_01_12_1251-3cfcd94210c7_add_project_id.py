"""add project id

Revision ID: 3cfcd94210c7
Revises: c2c4cb18a0fe
Create Date: 2026-01-12 12:51:42.433701

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "3cfcd94210c7"
down_revision: Union[str, None] = "c2c4cb18a0fe"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "events", sa.Column("project_id", sa.UUID(as_uuid=True), nullable=True)
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("events", "project_id")
