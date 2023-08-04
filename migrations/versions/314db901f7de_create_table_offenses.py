"""create table offenses

Revision ID: 314db901f7de
Revises: d091e458d7f6
Create Date: 2023-08-04 10:31:30.388003

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '314db901f7de'
down_revision = 'd091e458d7f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE offenses (
        id SERIAL PRIMARY KEY, 
        code INT NOT NULL UNIQUE, 
        code_group TEXT NOT NULL,
        description TEXT
        );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS offenses CASCADE;
""")