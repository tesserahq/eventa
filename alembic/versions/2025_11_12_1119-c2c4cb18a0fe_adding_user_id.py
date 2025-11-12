"""Adding user id

Revision ID: c2c4cb18a0fe
Revises: initialize_database
Create Date: 2025-11-12 11:19:46.502075

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c2c4cb18a0fe"
down_revision: Union[str, None] = "initialize_database"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("events", sa.Column("user_id", sa.UUID(as_uuid=True), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("events", "user_id")
