"""SQLAlchemy models for PSAT job queue and artifact storage."""

from __future__ import annotations

import enum
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import (
    Column,
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
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker


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

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    address = Column(String(42), nullable=True)
    company = Column(String, nullable=True)
    name = Column(String, nullable=True)
    status = Column(Enum(JobStatus), nullable=False, default=JobStatus.queued)
    stage = Column(Enum(JobStage), nullable=False, default=JobStage.discovery)
    detail = Column(Text, nullable=True)
    request = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)
    worker_id = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    artifacts = relationship("Artifact", back_populates="job", cascade="all, delete-orphan")
    source_files = relationship("SourceFile", back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (Index("ix_jobs_stage_status", "stage", "status"),)

    def to_dict(self) -> dict:
        return {
            "job_id": str(self.id),
            "address": self.address,
            "company": self.company,
            "name": self.name,
            "status": self.status.value if self.status else None,
            "stage": self.stage.value if self.stage else None,
            "detail": self.detail,
            "request": self.request,
            "error": self.error,
            "worker_id": self.worker_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Artifact(Base):
    __tablename__ = "artifacts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    data = Column(JSONB, nullable=True)
    text_data = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    job = relationship("Job", back_populates="artifacts")

    __table_args__ = (UniqueConstraint("job_id", "name", name="uq_artifact_job_name"),)


class SourceFile(Base):
    __tablename__ = "source_files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    path = Column(String, nullable=False)
    content = Column(Text, nullable=False)

    job = relationship("Job", back_populates="source_files")


load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://psat:psat@localhost:5433/psat")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)
