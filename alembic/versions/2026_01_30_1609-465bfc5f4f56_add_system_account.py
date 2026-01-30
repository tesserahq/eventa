"""add system account

Revision ID: 465bfc5f4f56
Revises: 3cfcd94210c7
Create Date: 2026-01-30 16:09:34.047702

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "465bfc5f4f56"
down_revision: Union[str, None] = "3cfcd94210c7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("users", sa.Column("service_account", sa.Boolean, default=False))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("users", "service_account")
