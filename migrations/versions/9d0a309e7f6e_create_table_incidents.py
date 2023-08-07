"""create table incidents

Revision ID: 9d0a309e7f6e
Revises: 314db901f7de
Create Date: 2023-08-04 10:31:35.000875

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9d0a309e7f6e'
down_revision = '314db901f7de'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE incidents (
        id SERIAL PRIMARY KEY, 
        number TEXT NOT NULL UNIQUE, 
        offense_code INT REFERENCES offenses(code) ON DELETE CASCADE,
        area_id INT REFERENCES areas(id) ON DELETE CASCADE, 
        shooting INT NOT NULL,
        date DATE NOT NULL,
        time TEXT NOT NULL,
        day_of_week INT NOT NULL
        
        
        );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS incidents CASCADE;
""")