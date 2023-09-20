"""create table areas

Revision ID: ac37ace4a950
Revises: 85ae6c78c35b
Create Date: 2023-09-20 18:34:20.862208

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ac37ace4a950'
down_revision = '85ae6c78c35b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE IF NOT EXISTS  areas (
        reporting_area INT PRIMARY KEY,
        street TEXT NOT NULL,
        district VARCHAR(3) CHECK (LENGTH(district) = 3)
    );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS areas;
""")