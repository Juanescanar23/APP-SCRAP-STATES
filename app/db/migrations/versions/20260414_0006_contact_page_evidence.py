from __future__ import annotations

from alembic import op
from sqlalchemy.dialects import postgresql

revision = "20260414_0006"
down_revision = "20260413_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE contactkind ADD VALUE IF NOT EXISTS 'contact_page'")


def downgrade() -> None:
    bind = op.get_bind()
    downgraded_contact_kind = postgresql.ENUM(
        "email",
        "contact_form",
        "phone",
        name="contactkind",
        create_type=False,
    )

    op.execute(
        "ALTER TABLE contact_evidence ALTER COLUMN kind TYPE text USING kind::text"
    )
    op.execute(
        "ALTER TABLE suppression_entry ALTER COLUMN channel_kind TYPE text USING channel_kind::text"
    )
    op.execute(
        "ALTER TABLE opt_out_log ALTER COLUMN channel_kind TYPE text USING channel_kind::text"
    )

    op.execute("UPDATE contact_evidence SET kind = 'contact_form' WHERE kind = 'contact_page'")
    op.execute(
        "UPDATE suppression_entry "
        "SET channel_kind = 'contact_form' "
        "WHERE channel_kind = 'contact_page'"
    )
    op.execute(
        "UPDATE opt_out_log "
        "SET channel_kind = 'contact_form' "
        "WHERE channel_kind = 'contact_page'"
    )

    op.execute("ALTER TYPE contactkind RENAME TO contactkind_old")
    downgraded_contact_kind.create(bind, checkfirst=False)

    op.execute(
        "ALTER TABLE contact_evidence ALTER COLUMN kind TYPE contactkind USING kind::contactkind"
    )
    op.execute(
        "ALTER TABLE suppression_entry "
        "ALTER COLUMN channel_kind TYPE contactkind "
        "USING channel_kind::contactkind"
    )
    op.execute(
        "ALTER TABLE opt_out_log "
        "ALTER COLUMN channel_kind TYPE contactkind "
        "USING channel_kind::contactkind"
    )

    op.execute("DROP TYPE contactkind_old")
