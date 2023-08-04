"""create table areas

Revision ID: d091e458d7f6
Revises: 
Create Date: 2023-08-04 10:31:23.179149

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd091e458d7f6'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE areas (
        id SERIAL PRIMARY KEY, 
        street TEXT NOT NULL, 
        reporting_area INT,
        district VARCHAR(3)
        );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS areas CASCADE;
""")
