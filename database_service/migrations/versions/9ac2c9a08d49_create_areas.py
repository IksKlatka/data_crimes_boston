"""create_areas

Revision ID: 9ac2c9a08d49
Revises: 09f988fbc691
Create Date: 2023-09-06 11:02:14.390550

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9ac2c9a08d49'
down_revision = '09f988fbc691'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE IF NOT EXISTS  areas (
        id SERIAL PRIMARY KEY,
        street TEXT NOT NULL,
        reporting_area INT NOT NULL, 
        district VARCHAR(3) CHECK (LENGTH(district) = 3)
    );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS areas;
""")
