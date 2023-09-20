"""create table offenses

Revision ID: 55fd5482d359
Revises: 
Create Date: 2023-09-20 18:27:51.753061

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '55fd5482d359'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE IF NOT EXISTS offenses(
        id SERIAL PRIMARY KEY,
        code INT NOT NULL UNIQUE,
        code_group VARCHAR(32) NOT NULL,
        description TEXT NOT NULL
    );
    
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS offenses;
""")
