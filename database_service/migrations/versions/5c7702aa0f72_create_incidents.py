"""initial_migration

Revision ID: 5c7702aa0f72
Revises: 
Create Date: 2023-09-06 10:03:04.354928

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5c7702aa0f72'
down_revision = '9ac2c9a08d49'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    CREATE TABLE IF NOT EXISTS incidents(
        id SERIAL PRIMARY KEY,
        incident_num TEXT NOT NULL UNIQUE, 
        offense_id INT REFERENCES offenses(id) ON DELETE CASCADE,
        area_id INT REFERENCES areas(id) ON DELETE CASCADE,
        shooting NUMERIC CHECK (shooting = 0 OR shooting = 1),
        date DATE NOT NULL,
        time TIME NOT NULL, 
        day_of_week INT NOT NULL CHECK (day_of_week > 0 AND day_of_week < 8)
        );
""")


def downgrade() -> None:
    op.execute(f"""
    DROP TABLE IF EXISTS incidents;
""")
