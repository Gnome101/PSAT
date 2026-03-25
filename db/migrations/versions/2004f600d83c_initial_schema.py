"""initial schema

Revision ID: 2004f600d83c
Revises:
Create Date: 2026-03-24 17:00:33.159401

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "2004f600d83c"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Create enums explicitly so they can be reused
job_status_enum = postgresql.ENUM("queued", "processing", "completed", "failed", name="jobstatus", create_type=False)
job_stage_enum = postgresql.ENUM(
    "discovery", "static", "resolution", "policy", "done", name="jobstage", create_type=False
)


def upgrade() -> None:
    """Create jobs, artifacts, and source_files tables."""
    # Create enum types
    job_status_enum.create(op.get_bind(), checkfirst=True)
    job_stage_enum.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("address", sa.String(42), nullable=True),
        sa.Column("company", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("status", job_status_enum, nullable=False, server_default="queued"),
        sa.Column("stage", job_stage_enum, nullable=False, server_default="discovery"),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("request", postgresql.JSONB(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("worker_id", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_jobs_stage_status", "jobs", ["stage", "status"])

    op.create_table(
        "artifacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("data", postgresql.JSONB(), nullable=True),
        sa.Column("text_data", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("job_id", "name", name="uq_artifact_job_name"),
    )

    op.create_table(
        "source_files",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
    )


def downgrade() -> None:
    """Drop tables and enum types."""
    op.drop_table("source_files")
    op.drop_table("artifacts")
    op.drop_index("ix_jobs_stage_status", table_name="jobs")
    op.drop_table("jobs")

    job_stage_enum.drop(op.get_bind(), checkfirst=True)
    job_status_enum.drop(op.get_bind(), checkfirst=True)
