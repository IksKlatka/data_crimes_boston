"""create_offenses

Revision ID: 09f988fbc691
Revises: 5c7702aa0f72
Create Date: 2023-09-06 10:58:25.792248

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '09f988fbc691'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE IF NOT EXISTS offenses (
        id SERIAL PRIMARY KEY, 
        code INT NOT NULL UNIQUE,
        code_group TEXT NOT NULL,
        description TEXT NOT NULL    
    );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS offenses;
""")
