"""alter incidents reference reporting offense

Revision ID: 206d675d085e
Revises: ac37ace4a950
Create Date: 2023-09-20 18:36:50.332019

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '206d675d085e'
down_revision = 'ac37ace4a950'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
    ALTER TABLE incidents
    ADD CONSTRAINT reporting_area_fk
    FOREIGN KEY (reporting_area) REFERENCES areas(reporting_area);
""")


def downgrade() -> None:
    op.execute(f"""
    ALTER TABLE incidents 
    DROP CONSTRAINT reporting_area_fk; 
""")
