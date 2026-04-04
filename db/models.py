"""SQLAlchemy models for PSAT job queue and artifact storage."""

from __future__ import annotations

import enum
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    queued = "queued"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class JobStage(str, enum.Enum):
    discovery = "discovery"
    static = "static"
    resolution = "resolution"
    policy = "policy"
    done = "done"


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    address: Mapped[str | None] = mapped_column(String(42), nullable=True)
    company: Mapped[str | None] = mapped_column(String, nullable=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), nullable=False, default=JobStatus.queued)
    stage: Mapped[JobStage] = mapped_column(Enum(JobStage), nullable=False, default=JobStage.discovery)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    request: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    artifacts: Mapped[list["Artifact"]] = relationship("Artifact", back_populates="job", cascade="all, delete-orphan")
    source_files: Mapped[list["SourceFile"]] = relationship(
        "SourceFile", back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (Index("ix_jobs_stage_status", "stage", "status"),)

    def to_dict(self) -> dict:
        return {
            "job_id": str(self.id),
            "address": self.address,
            "company": self.company,
            "name": self.name,
            "status": self.status.value,
            "stage": self.stage.value,
            "detail": self.detail,
            "request": self.request,
            "error": self.error,
            "worker_id": self.worker_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    data: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    text_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    job: Mapped[Job] = relationship("Job", back_populates="artifacts")

    __table_args__ = (UniqueConstraint("job_id", "name", name="uq_artifact_job_name"),)


class SourceFile(Base):
    __tablename__ = "source_files"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    path: Mapped[str] = mapped_column(String, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)

    job: Mapped[Job] = relationship("Job", back_populates="source_files")


class WatchedProxy(Base):
    __tablename__ = "watched_proxies"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    proxy_address: Mapped[str] = mapped_column(String(42), nullable=False)
    chain: Mapped[str] = mapped_column(String, nullable=False, default="ethereum")
    label: Mapped[str | None] = mapped_column(String, nullable=True)
    last_known_implementation: Mapped[str | None] = mapped_column(String(42), nullable=True)
    last_scanned_block: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    events: Mapped[list["ProxyUpgradeEvent"]] = relationship(
        "ProxyUpgradeEvent", back_populates="watched_proxy", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("proxy_address", "chain", name="uq_watched_proxy_address_chain"),)


class ProxyUpgradeEvent(Base):
    __tablename__ = "proxy_upgrade_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    watched_proxy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("watched_proxies.id", ondelete="CASCADE"), nullable=False
    )
    block_number: Mapped[int] = mapped_column(nullable=False)
    tx_hash: Mapped[str] = mapped_column(String(66), nullable=False)
    old_implementation: Mapped[str | None] = mapped_column(String(42), nullable=True)
    new_implementation: Mapped[str] = mapped_column(String(42), nullable=False)
    event_type: Mapped[str] = mapped_column(String, nullable=False, default="upgraded")
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    watched_proxy: Mapped[WatchedProxy] = relationship("WatchedProxy", back_populates="events")


load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://psat:psat@localhost:5433/psat")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)
