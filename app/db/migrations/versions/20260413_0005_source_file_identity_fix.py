from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260413_0005"
down_revision = "20260413_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE source_file
        SET file_date = make_date(
            extract(year from downloaded_at at time zone 'UTC')::int,
            (((extract(quarter from downloaded_at at time zone 'UTC')::int - 1) * 3) + 1),
            1
        )
        WHERE file_date IS NULL
          AND source_kind IN ('quarterly_corporate', 'quarterly_corporate_events')
        """
    )

    op.drop_constraint(
        "uq_source_file_provider_kind_filename",
        "source_file",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_source_file_provider_kind_filename_file_date_checksum",
        "source_file",
        ["provider", "source_kind", "filename", "file_date", "source_checksum"],
    )
    op.create_index(
        "ix_source_file_provider_kind_filename_file_date",
        "source_file",
        ["provider", "source_kind", "filename", "file_date"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_source_file_provider_kind_filename_file_date", table_name="source_file")
    op.drop_constraint(
        "uq_source_file_provider_kind_filename_file_date_checksum",
        "source_file",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_source_file_provider_kind_filename",
        "source_file",
        ["provider", "source_kind", "filename"],
    )
