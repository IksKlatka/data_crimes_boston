"""create table incidents

Revision ID: 85ae6c78c35b
Revises: 55fd5482d359
Create Date: 2023-09-20 18:30:31.702359

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '85ae6c78c35b'
down_revision = '55fd5482d359'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS incidents(
        id SERIAL PRIMARY KEY,
        incident_number VARCHAR(16) UNIQUE NOT NULL,
        offense_code INT NOT NULL REFERENCES offenses(code),
        reporting_area INT NOT NULL,
        shooting BOOLEAN NOT NULL,
        date DATE NOT NULL,
        time VARCHAR(9) NOT NULL,
        day_of_week INT NOT NULL CHECK (day_of_week > 0 AND day_of_week < 8)
        );
    """)


def downgrade() -> None:
    op.execute(f"""
        DROP TABLE IF EXISTS incidents;
    """)