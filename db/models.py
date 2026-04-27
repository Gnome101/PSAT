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
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
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
    dapp_crawl = "dapp_crawl"
    defillama_scan = "defillama_scan"
    selection = "selection"
    static = "static"
    resolution = "resolution"
    policy = "policy"
    coverage = "coverage"
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
    protocol_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("protocols.id", ondelete="SET NULL"), nullable=True
    )
    # Mirrored from contract_flags by store_artifact; lets /api/jobs skip the artifact resolve.
    is_proxy: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
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
            "is_proxy": self.is_proxy,
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
    # Legacy inline storage. Kept nullable so pre-Tigris rows still read; new writes leave NULL.
    data: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    text_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
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
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)

    job: Mapped[Job] = relationship("Job", back_populates="source_files")


class WatchedProxy(Base):
    __tablename__ = "watched_proxies"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    proxy_address: Mapped[str] = mapped_column(String(42), nullable=False)
    chain: Mapped[str] = mapped_column(String, nullable=False, default="ethereum")
    label: Mapped[str | None] = mapped_column(String, nullable=True)
    proxy_type: Mapped[str | None] = mapped_column(String, nullable=True)
    last_known_implementation: Mapped[str | None] = mapped_column(String(42), nullable=True)
    last_scanned_block: Mapped[int] = mapped_column(default=0)
    needs_polling: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    events: Mapped[list["ProxyUpgradeEvent"]] = relationship(
        "ProxyUpgradeEvent", back_populates="watched_proxy", cascade="all, delete-orphan"
    )
    subscriptions: Mapped[list["ProxySubscription"]] = relationship(
        "ProxySubscription", back_populates="watched_proxy", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("proxy_address", "chain", name="uq_watched_proxy_address_chain"),)


class ProxySubscription(Base):
    __tablename__ = "proxy_subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    watched_proxy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("watched_proxies.id", ondelete="CASCADE"), nullable=False
    )
    discord_webhook_url: Mapped[str | None] = mapped_column(String, nullable=True)
    label: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    watched_proxy: Mapped[WatchedProxy] = relationship("WatchedProxy", back_populates="subscriptions")


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


# ---------------------------------------------------------------------------
# Protocol / company entity
# ---------------------------------------------------------------------------


class Protocol(Base):
    __tablename__ = "protocols"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    chains: Mapped[list[str] | None] = mapped_column(ARRAY(String(100)), server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    contracts: Mapped[list["Contract"]] = relationship("Contract", back_populates="protocol")
    monitored_contracts: Mapped[list["MonitoredContract"]] = relationship(
        "MonitoredContract", backref="protocol", foreign_keys="MonitoredContract.protocol_id"
    )
    protocol_subscriptions: Mapped[list["ProtocolSubscription"]] = relationship(
        "ProtocolSubscription", backref="protocol"
    )
    official_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)

    audit_reports: Mapped[list["AuditReport"]] = relationship(
        "AuditReport", backref="protocol", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("name", name="uq_protocol_name"),)


class AuditReport(Base):
    __tablename__ = "audit_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    protocol_id: Mapped[int] = mapped_column(Integer, ForeignKey("protocols.id", ondelete="CASCADE"), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    pdf_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    auditor: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    date: Mapped[str | None] = mapped_column(String(20), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Numeric(5, 4), nullable=True)

    # Text-extraction pipeline state. Populated by workers.audit_text_extraction.
    # status values: NULL (not yet attempted), "processing", "success",
    # "failed", "skipped" (e.g. image-only PDFs that need OCR).
    text_extraction_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    text_extraction_worker: Mapped[str | None] = mapped_column(String(128), nullable=True)
    text_extraction_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    text_extracted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    text_extraction_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    text_storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    text_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    text_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Scope-extraction pipeline state. Populated by workers.audit_scope_extraction
    # once text_extraction_status='success'. Mirrors the text_* state machine:
    # NULL (eligible) -> "processing" -> "success"/"failed"/"skipped".
    # "skipped" means no scope-section header was found in the PDF text.
    scope_extraction_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    scope_extraction_worker: Mapped[str | None] = mapped_column(String(128), nullable=True)
    scope_extraction_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scope_extracted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scope_extraction_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    scope_storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    scope_contracts: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True)

    # Commit SHAs mentioned in the PDF as the reviewed revision.
    reviewed_commits: Mapped[list[str] | None] = mapped_column(ARRAY(String(40)), nullable=True)
    # Lower-cased fallback GitHub repos mentioned anywhere in the PDF body.
    referenced_repos: Mapped[list[str] | None] = mapped_column(ARRAY(String(255)), nullable=True)
    # Phase C: LLM-labeled commit metadata from the audit text.
    classified_commits: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)

    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    # GitHub repo the PDF was discovered in.
    source_repo: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # Findings extracted from the audit; stored as JSONB so the shape can evolve.
    findings: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    # Structured scope-table rows, kept alongside the flat ``scope_contracts`` list.
    scope_entries: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("protocol_id", "url", name="uq_audit_report_protocol_url"),
        Index("ix_audit_reports_protocol_id", "protocol_id"),
        Index(
            "ix_audit_reports_text_extraction_status",
            "text_extraction_status",
        ),
        Index(
            "ix_audit_reports_scope_extraction_status",
            "scope_extraction_status",
        ),
    )


class AuditContractCoverage(Base):
    """Link between an ``AuditReport`` and a ``Contract`` that was in scope.

    Persisted so "which audits cover this impl?" is a plain join, not a
    query-time scan of ``scope_contracts[]``. Proxy-aware: the row links
    the implementation-era ``Contract`` the audit actually reviewed, not
    the proxy. See ``services.audits.coverage`` for the matcher.
    """

    __tablename__ = "audit_contract_coverage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    audit_report_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("audit_reports.id", ondelete="CASCADE"), nullable=False
    )
    # Denormalized FK so per-protocol queries stay single-hop.
    protocol_id: Mapped[int] = mapped_column(Integer, ForeignKey("protocols.id", ondelete="CASCADE"), nullable=False)
    # Matching scope entry, kept for debugging and auditability.
    matched_name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Match taxonomy lives in ``services.audits.coverage``.
    match_type: Mapped[str] = mapped_column(String(32), nullable=False)
    # String enum to avoid implying false numeric precision downstream.
    match_confidence: Mapped[str] = mapped_column(String(10), nullable=False)
    # Impl active window the audit applies to. NULL for direct matches.
    covered_from_block: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    covered_to_block: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # Runtime bytecode anchor captured when the coverage row was written.
    bytecode_keccak_at_match: Mapped[str | None] = mapped_column(String(66), nullable=True)
    # Timestamp for the bytecode anchor sample.
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Source-equivalence verdict for this (audit, contract) pair.
    equivalence_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    # Short human-readable detail for the equivalence verdict.
    equivalence_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Last verification attempt time, distinct from the bytecode anchor sample.
    equivalence_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Phase C proof strength for ``equivalence_status='proven'`` rows.
    proof_kind: Mapped[str | None] = mapped_column(String(30), nullable=True)
    # Specific commit SHA from ``AuditReport.classified_commits`` that matched
    # this contract's bytecode during verification. Populated alongside
    # ``proof_kind``/``equivalence_status``. NULL for heuristic-only matches
    # (direct / impl_era) and for rows verified before this field existed.
    # Stored as the full 40-char hex so downstream can build GitHub tree URLs
    # without having to look up the audit's commit list again.
    matched_commit_sha: Mapped[str | None] = mapped_column(String(66), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("contract_id", "audit_report_id", name="uq_audit_contract_coverage_pair"),
        Index("ix_audit_contract_coverage_contract_id", "contract_id"),
        Index("ix_audit_contract_coverage_audit_report_id", "audit_report_id"),
        Index("ix_audit_contract_coverage_protocol_id", "protocol_id"),
    )


# ---------------------------------------------------------------------------
# Pipeline artifact tables (replace JSONB blobs)
# ---------------------------------------------------------------------------


class Contract(Base):
    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True
    )
    protocol_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("protocols.id", ondelete="SET NULL"), nullable=True
    )
    address: Mapped[str] = mapped_column(String(42), nullable=False)
    source_verified: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    chain: Mapped[str | None] = mapped_column(String(100), nullable=True)
    contract_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    compiler_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    language: Mapped[str | None] = mapped_column(String(20), nullable=True)
    evm_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    optimization: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    optimization_runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_format: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source_file_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    license: Mapped[str | None] = mapped_column(String(100), nullable=True)
    is_proxy: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    proxy_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    implementation: Mapped[str | None] = mapped_column(String(42), nullable=True)
    beacon: Mapped[str | None] = mapped_column(String(42), nullable=True)
    admin: Mapped[str | None] = mapped_column(String(42), nullable=True)
    deployer: Mapped[str | None] = mapped_column(String(42), nullable=True)
    remappings: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True)
    rank_score: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    # Every source that has independently confirmed this contract for the
    # protocol. Writers union their tag in instead of overwriting, so
    # ranking can boost contracts corroborated by multiple discovery
    # pipelines (e.g. shown on the docs page AND called by the DApp).
    discovery_sources: Mapped[list[str] | None] = mapped_column(ARRAY(String(100)), nullable=True)
    discovery_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    chains: Mapped[list[str] | None] = mapped_column(ARRAY(String(100)), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    job: Mapped[Job] = relationship("Job")
    protocol: Mapped[Protocol | None] = relationship("Protocol", back_populates="contracts")
    summary: Mapped["ContractSummary | None"] = relationship(
        "ContractSummary", back_populates="contract", uselist=False, cascade="all, delete-orphan"
    )
    privileged_functions: Mapped[list["PrivilegedFunction"]] = relationship(
        "PrivilegedFunction", back_populates="contract", cascade="all, delete-orphan"
    )
    role_definitions: Mapped[list["RoleDefinition"]] = relationship(
        "RoleDefinition", back_populates="contract", cascade="all, delete-orphan"
    )
    controller_values: Mapped[list["ControllerValue"]] = relationship(
        "ControllerValue", back_populates="contract", cascade="all, delete-orphan"
    )
    control_graph_nodes: Mapped[list["ControlGraphNode"]] = relationship(
        "ControlGraphNode", back_populates="contract", cascade="all, delete-orphan"
    )
    control_graph_edges: Mapped[list["ControlGraphEdge"]] = relationship(
        "ControlGraphEdge", back_populates="contract", cascade="all, delete-orphan"
    )
    upgrade_events: Mapped[list["UpgradeEvent"]] = relationship(
        "UpgradeEvent", back_populates="contract", cascade="all, delete-orphan"
    )
    effective_functions: Mapped[list["EffectiveFunction"]] = relationship(
        "EffectiveFunction", back_populates="contract", cascade="all, delete-orphan"
    )
    principal_labels: Mapped[list["PrincipalLabel"]] = relationship(
        "PrincipalLabel", back_populates="contract", cascade="all, delete-orphan"
    )
    dependencies: Mapped[list["ContractDependency"]] = relationship(
        "ContractDependency", back_populates="contract", cascade="all, delete-orphan"
    )
    balances: Mapped[list["ContractBalance"]] = relationship(
        "ContractBalance", back_populates="contract", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_contracts_job_id", "job_id"),
        Index("ix_contracts_protocol_id", "protocol_id"),
        UniqueConstraint("address", "chain", name="uq_contract_address_chain"),
    )


class ContractSummary(Base):
    __tablename__ = "contract_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    control_model: Mapped[str | None] = mapped_column(String(50), nullable=True)
    is_upgradeable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_pausable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    has_timelock: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    risk_level: Mapped[str | None] = mapped_column(String(20), nullable=True)
    is_factory: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_nft: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    standards: Mapped[list[str] | None] = mapped_column(ARRAY(String(50)), nullable=True)
    source_verified: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="summary")


class PrivilegedFunction(Base):
    __tablename__ = "privileged_functions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    function_name: Mapped[str] = mapped_column(String(255), nullable=False)
    selector: Mapped[str | None] = mapped_column(String(10), nullable=True)
    abi_signature: Mapped[str | None] = mapped_column(Text, nullable=True)
    effect_labels: Mapped[list[str] | None] = mapped_column(ARRAY(String(100)), nullable=True)
    action_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    authority_public: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    contract: Mapped[Contract] = relationship("Contract", back_populates="privileged_functions")

    __table_args__ = (Index("ix_privileged_functions_contract_id", "contract_id"),)


class RoleDefinition(Base):
    __tablename__ = "role_definitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    role_name: Mapped[str] = mapped_column(String(255), nullable=False)
    declared_in: Mapped[str | None] = mapped_column(String(255), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="role_definitions")

    __table_args__ = (Index("ix_role_definitions_contract_id", "contract_id"),)


class ControllerValue(Base):
    __tablename__ = "controller_values"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    controller_id: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[str | None] = mapped_column(String(66), nullable=True)
    resolved_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    block_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    details: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    observed_via: Mapped[str | None] = mapped_column(String(100), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="controller_values")

    __table_args__ = (Index("ix_controller_values_contract_id", "contract_id"),)


class ControlGraphNode(Base):
    __tablename__ = "control_graph_nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    address: Mapped[str] = mapped_column(String(42), nullable=False)
    node_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    resolved_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    contract_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    depth: Mapped[int | None] = mapped_column(Integer, nullable=True)
    analyzed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    details: Mapped[Any | None] = mapped_column(JSONB, nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="control_graph_nodes")

    __table_args__ = (Index("ix_control_graph_nodes_contract_id", "contract_id"),)


class ControlGraphEdge(Base):
    __tablename__ = "control_graph_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    from_node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    to_node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    relation: Mapped[str | None] = mapped_column(String(100), nullable=True)
    label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_controller_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    notes: Mapped[list[str] | None] = mapped_column(ARRAY(String), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="control_graph_edges")

    __table_args__ = (Index("ix_control_graph_edges_contract_id", "contract_id"),)


class UpgradeEvent(Base):
    __tablename__ = "upgrade_events"
    __table_args__ = (Index("ix_upgrade_events_contract_id", "contract_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    proxy_address: Mapped[str] = mapped_column(String(42), nullable=False)
    old_impl: Mapped[str | None] = mapped_column(String(42), nullable=True)
    new_impl: Mapped[str | None] = mapped_column(String(42), nullable=True)
    block_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    tx_hash: Mapped[str | None] = mapped_column(String(66), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="upgrade_events")


class EffectiveFunction(Base):
    __tablename__ = "effective_functions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    function_name: Mapped[str] = mapped_column(String(255), nullable=False)
    selector: Mapped[str | None] = mapped_column(String(10), nullable=True)
    abi_signature: Mapped[str | None] = mapped_column(Text, nullable=True)
    effect_labels: Mapped[list[str] | None] = mapped_column(ARRAY(String(100)), nullable=True)
    effect_targets: Mapped[list[str] | None] = mapped_column(ARRAY(String(255)), nullable=True)
    action_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    authority_public: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    authority_roles: Mapped[Any | None] = mapped_column(JSONB, nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="effective_functions")
    principals: Mapped[list["FunctionPrincipal"]] = relationship(
        "FunctionPrincipal", back_populates="function", cascade="all, delete-orphan"
    )

    __table_args__ = (Index("ix_effective_functions_contract_id", "contract_id"),)


class FunctionPrincipal(Base):
    __tablename__ = "function_principals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    function_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("effective_functions.id", ondelete="CASCADE"), nullable=False
    )
    address: Mapped[str] = mapped_column(String(42), nullable=False)
    resolved_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    origin: Mapped[str | None] = mapped_column(String(255), nullable=True)
    principal_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    details: Mapped[Any | None] = mapped_column(JSONB, nullable=True)

    function: Mapped[EffectiveFunction] = relationship("EffectiveFunction", back_populates="principals")

    __table_args__ = (Index("ix_function_principals_function_id", "function_id"),)


class PrincipalLabel(Base):
    __tablename__ = "principal_labels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    address: Mapped[str] = mapped_column(String(42), nullable=False)
    label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    resolved_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    labels: Mapped[list[str] | None] = mapped_column(ARRAY(String(255)), nullable=True)
    confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)
    details: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    graph_context: Mapped[list[str] | None] = mapped_column(ARRAY(String(255)), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="principal_labels")

    __table_args__ = (Index("ix_principal_labels_contract_id", "contract_id"),)


class AddressLabel(Base):
    """Admin-curated human-readable name for an arbitrary address.

    Exists to give Safe signers and EOA principals — which are just raw
    addresses with no on-chain metadata — a legible name in the UI. Keyed
    by the lowercased address so it applies everywhere the address appears
    (signer list, EOA card, function guard, etc.), independent of any
    specific contract context. Distinct from ``PrincipalLabel`` which is
    worker-populated and scoped per-contract.
    """

    __tablename__ = "address_labels"

    address: Mapped[str] = mapped_column(String(42), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class ContractDependency(Base):
    __tablename__ = "contract_dependencies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    dependency_address: Mapped[str] = mapped_column(String(42), nullable=False)
    dependency_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    relationship_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source: Mapped[list[str] | None] = mapped_column(ARRAY(String(50)), nullable=True)
    proxy_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    implementation: Mapped[str | None] = mapped_column(String(42), nullable=True)
    admin: Mapped[str | None] = mapped_column(String(42), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="dependencies")

    __table_args__ = (Index("ix_contract_dependencies_contract_id", "contract_id"),)


# ---------------------------------------------------------------------------
# Unified monitoring tables
# ---------------------------------------------------------------------------


class MonitoredContract(Base):
    __tablename__ = "monitored_contracts"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    address: Mapped[str] = mapped_column(String(42), nullable=False)
    chain: Mapped[str] = mapped_column(String(100), nullable=False, default="ethereum")
    protocol_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("protocols.id", ondelete="SET NULL"), nullable=True
    )
    contract_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("contracts.id", ondelete="SET NULL"), nullable=True
    )
    watched_proxy_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("watched_proxies.id", ondelete="SET NULL"), nullable=True
    )
    contract_type: Mapped[str] = mapped_column(String(50), nullable=False, default="regular")
    monitoring_config: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=True
    )
    last_known_state: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=True
    )
    last_scanned_block: Mapped[int] = mapped_column(Integer, default=0)
    needs_polling: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    enrollment_source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    events: Mapped[list["MonitoredEvent"]] = relationship(
        "MonitoredEvent", back_populates="monitored_contract", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("address", "chain", name="uq_monitored_contract_address_chain"),
        Index("ix_monitored_contracts_protocol_id", "protocol_id"),
    )


class MonitoredEvent(Base):
    __tablename__ = "monitored_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    monitored_contract_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("monitored_contracts.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    block_number: Mapped[int] = mapped_column(Integer, nullable=False)
    tx_hash: Mapped[str] = mapped_column(String(66), nullable=False)
    data: Mapped[dict[str, Any] | None] = mapped_column(JSON().with_variant(JSONB(), "postgresql"), nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    monitored_contract: Mapped[MonitoredContract] = relationship("MonitoredContract", back_populates="events")

    __table_args__ = (
        Index("ix_monitored_events_contract_id", "monitored_contract_id"),
        Index("ix_monitored_events_event_type", "event_type"),
        Index("ix_monitored_events_detected_at", "detected_at"),
    )


class ProtocolSubscription(Base):
    __tablename__ = "protocol_subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    protocol_id: Mapped[int] = mapped_column(Integer, ForeignKey("protocols.id", ondelete="CASCADE"), nullable=False)
    discord_webhook_url: Mapped[str | None] = mapped_column(String, nullable=True)
    label: Mapped[str | None] = mapped_column(String, nullable=True)
    event_filter: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (Index("ix_protocol_subscriptions_protocol_id", "protocol_id"),)


class ContractBalance(Base):
    __tablename__ = "contract_balances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    token_address: Mapped[str | None] = mapped_column(String(42), nullable=True)  # NULL = native ETH
    token_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    token_symbol: Mapped[str | None] = mapped_column(String(50), nullable=True)
    decimals: Mapped[int] = mapped_column(Integer, nullable=False, default=18)
    raw_balance: Mapped[str] = mapped_column(String, nullable=False)  # stored as string to avoid overflow
    usd_value: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    price_usd: Mapped[float | None] = mapped_column(Numeric(20, 8), nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    contract: Mapped[Contract] = relationship("Contract", back_populates="balances")

    __table_args__ = (Index("ix_contract_balances_contract_id", "contract_id"),)


class DAppInteraction(Base):
    __tablename__ = "dapp_interactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    protocol_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("protocols.id", ondelete="SET NULL"), nullable=True
    )
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    page_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_address: Mapped[str | None] = mapped_column(String(42), nullable=True)
    value: Mapped[str | None] = mapped_column(String(80), nullable=True)
    data: Mapped[str | None] = mapped_column(Text, nullable=True)
    method_selector: Mapped[str | None] = mapped_column(String(10), nullable=True)
    typed_data: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    is_permit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    captured_at: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_dapp_interactions_job_id", "job_id"),
        Index("ix_dapp_interactions_to_address", "to_address"),
        Index("ix_dapp_interactions_protocol_id", "protocol_id"),
    )


class TvlSnapshot(Base):
    __tablename__ = "tvl_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    protocol_id: Mapped[int] = mapped_column(Integer, ForeignKey("protocols.id", ondelete="CASCADE"), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    total_usd: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    defillama_tvl: Mapped[float | None] = mapped_column(Numeric(20, 2), nullable=True)
    chain_breakdown: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=True
    )
    contract_breakdown: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(JSONB(), "postgresql"), nullable=True
    )
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="on_chain")

    __table_args__ = (Index("ix_tvl_snapshots_protocol_timestamp", "protocol_id", "timestamp"),)


load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://psat:psat@localhost:5433/psat")

# Defaults (5+10) serialize concurrent /api/company hits; env-overridable for tight-quota Postgres.
DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(os.environ.get("DB_MAX_OVERFLOW", "20"))
DB_POOL_RECYCLE = int(os.environ.get("DB_POOL_RECYCLE", "1800"))

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_recycle=DB_POOL_RECYCLE,
    # psycopg2 defaults connect_timeout to infinity — would block every
    # session acquisition during a Neon cold-start.
    connect_args={"connect_timeout": 10},
)
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


def apply_storage_migrations(target_engine=None) -> None:
    """Add object-storage columns to existing artifact and source_files tables.

    Idempotent — every ALTER uses IF NOT EXISTS or is a NULL-relaxing change.
    Called from create_tables() during app startup and from test fixtures so
    a TEST_DATABASE_URL that already has the old schema picks up the new columns.
    """
    from sqlalchemy import text

    target = target_engine if target_engine is not None else engine
    # ``ALTER TYPE ... ADD VALUE`` cannot run inside a transaction block on
    # Postgres, so run it on a dedicated AUTOCOMMIT connection. ``IF NOT
    # EXISTS`` keeps it idempotent across restarts and test-DB reuse.
    with target.connect().execution_options(isolation_level="AUTOCOMMIT") as ac_conn:
        # Fail fast instead of waiting forever for the enum's
        # AccessExclusiveLock held by the draining machine during a
        # rolling deploy.
        ac_conn.execute(text("SET lock_timeout = '10s'"))
        ac_conn.execute(text("SET statement_timeout = '30s'"))
        ac_conn.execute(text("ALTER TYPE jobstage ADD VALUE IF NOT EXISTS 'coverage' BEFORE 'done'"))
        ac_conn.execute(text("ALTER TYPE jobstage ADD VALUE IF NOT EXISTS 'selection' BEFORE 'static'"))
        # Contracts.discovery_source → discovery_sources (array): writers
        # now union their tag in so ranking can boost contracts
        # corroborated by multiple pipelines. Check whether the legacy
        # scalar still exists before trying to backfill from it — on
        # fresh databases or on second-run migrations the column is
        # already gone and referencing it in the UPDATE would raise.
        ac_conn.execute(text("ALTER TABLE contracts ADD COLUMN IF NOT EXISTS discovery_sources VARCHAR(100)[]"))
        has_legacy_column = ac_conn.execute(
            text(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_name = 'contracts' AND column_name = 'discovery_source'"
            )
        ).scalar()
        if has_legacy_column:
            ac_conn.execute(
                text(
                    "UPDATE contracts SET discovery_sources = ARRAY[discovery_source] "
                    "WHERE discovery_sources IS NULL AND discovery_source IS NOT NULL"
                )
            )
            ac_conn.execute(text("ALTER TABLE contracts DROP COLUMN discovery_source"))
    with target.connect() as conn:
        conn.execute(text("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS storage_key VARCHAR(512)"))
        conn.execute(text("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS size_bytes BIGINT"))
        conn.execute(text("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS content_type VARCHAR(64)"))
        conn.execute(text("ALTER TABLE source_files ADD COLUMN IF NOT EXISTS storage_key VARCHAR(512)"))
        conn.execute(text("ALTER TABLE source_files ALTER COLUMN content DROP NOT NULL"))
        # Text-extraction columns added to audit_reports after the first release
        # of that table. Older test DBs miss these; create_all() is a no-op
        # once the table exists, so apply them here idempotently.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_extraction_status VARCHAR(20)"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_extraction_worker VARCHAR(128)"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_extraction_started_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_extracted_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_extraction_error TEXT"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_storage_key TEXT"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_size_bytes INTEGER"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS text_sha256 VARCHAR(64)"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_reports_text_extraction_status "
                "ON audit_reports (text_extraction_status)"
            )
        )
        # Scope-extraction columns — added after text-extraction landed. Same
        # state-machine shape (status/worker/started_at/extracted_at/error)
        # plus a storage key for the per-audit JSON artifact and a denormalized
        # array of contract names that powers the /audit_coverage endpoint.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_extraction_status VARCHAR(20)"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_extraction_worker VARCHAR(128)"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_extraction_started_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_extracted_at TIMESTAMPTZ"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_extraction_error TEXT"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_storage_key TEXT"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_contracts TEXT[]"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_reports_scope_extraction_status "
                "ON audit_reports (scope_extraction_status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_reports_scope_contracts "
                "ON audit_reports USING GIN (scope_contracts)"
            )
        )
        # Partial index on text_sha256 for rows already scoped — powers the
        # content-hash cache lookup in the scope-extraction worker.
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_reports_text_sha256_scoped "
                "ON audit_reports (text_sha256) WHERE scope_extraction_status = 'success'"
            )
        )
        # reviewed_commits + source_repo support the source-equivalence
        # matcher — added idempotently so existing test DBs pick them up.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS reviewed_commits TEXT[]"))
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS source_repo VARCHAR(255)"))
        # referenced_repos (Phase D): every github.com/<owner>/<repo> URL
        # the PDF mentions. Fallback candidates for source-equivalence
        # when source_repo points at the wrong repo.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS referenced_repos TEXT[]"))
        # classified_commits (Phase C): LLM-labeled commits with roles
        # (reviewed / fix / cited / unclear) — drives ``proof_kind``
        # computation on coverage rows.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS classified_commits JSONB"))
        # findings jsonb — surfaces acknowledged/mitigated issues on the
        # current impl; Phase 3a seeds this manually, Phase 3b fills it from
        # scope extraction.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS findings JSONB"))
        # scope_entries jsonb — Phase F structured scope table: per-entry
        # (name, address, commit, chain) tuples for audits whose PDFs list
        # explicit addresses. Authoritative for the coverage matcher.
        conn.execute(text("ALTER TABLE audit_reports ADD COLUMN IF NOT EXISTS scope_entries JSONB"))
        # audit_contract_coverage — persistent contract↔audit link. Created
        # idempotently here so long-lived test DBs (and prod) pick it up
        # without an Alembic run. Base.metadata.create_all handles fresh
        # databases; this block handles upgrades.
        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS audit_contract_coverage ("
                "  id SERIAL PRIMARY KEY,"
                "  contract_id INTEGER NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,"
                "  audit_report_id INTEGER NOT NULL REFERENCES audit_reports(id) ON DELETE CASCADE,"
                "  protocol_id INTEGER NOT NULL REFERENCES protocols(id) ON DELETE CASCADE,"
                "  matched_name VARCHAR(255) NOT NULL,"
                "  match_type VARCHAR(32) NOT NULL,"
                "  match_confidence VARCHAR(10) NOT NULL,"
                "  covered_from_block BIGINT,"
                "  covered_to_block BIGINT,"
                "  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
                "  CONSTRAINT uq_audit_contract_coverage_pair "
                "    UNIQUE (contract_id, audit_report_id)"
                ")"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_contract_coverage_contract_id "
                "ON audit_contract_coverage (contract_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_contract_coverage_audit_report_id "
                "ON audit_contract_coverage (audit_report_id)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_audit_contract_coverage_protocol_id "
                "ON audit_contract_coverage (protocol_id)"
            )
        )
        # Bytecode anchor — runtime keccak of the impl at match time, so the
        # API can diff against live eth_getCode and flag drift (rare but real:
        # CREATE2 re-deploy at same address). NULL == "drift unknown", not
        # "drift detected".
        conn.execute(
            text("ALTER TABLE audit_contract_coverage ADD COLUMN IF NOT EXISTS bytecode_keccak_at_match VARCHAR(66)")
        )
        conn.execute(text("ALTER TABLE audit_contract_coverage ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ"))
        # Source-equivalence verdict + reason + last-checked timestamp. NULL
        # on existing rows until the next verify-enabled refresh populates
        # them (the worker sites already pass verify_source_equivalence=True).
        conn.execute(
            text("ALTER TABLE audit_contract_coverage ADD COLUMN IF NOT EXISTS equivalence_status VARCHAR(40)")
        )
        conn.execute(text("ALTER TABLE audit_contract_coverage ADD COLUMN IF NOT EXISTS equivalence_reason TEXT"))
        conn.execute(
            text("ALTER TABLE audit_contract_coverage ADD COLUMN IF NOT EXISTS equivalence_checked_at TIMESTAMPTZ")
        )
        # proof_kind (Phase C): strength subtype for proven rows. See the
        # model comment for the full vocabulary.
        conn.execute(text("ALTER TABLE audit_contract_coverage ADD COLUMN IF NOT EXISTS proof_kind VARCHAR(30)"))
        # jobs.is_proxy mirrors contract_flags.is_proxy. The UPDATE below only
        # backfills inline rows; storage-backed legacy rows stay False until
        # their contract_flags artifact is rewritten (re-analyze).
        conn.execute(text("ALTER TABLE jobs ADD COLUMN IF NOT EXISTS is_proxy BOOLEAN NOT NULL DEFAULT FALSE"))
        conn.execute(
            text(
                "UPDATE jobs SET is_proxy = TRUE "
                "FROM artifacts "
                "WHERE artifacts.job_id = jobs.id "
                "  AND artifacts.name = 'contract_flags' "
                "  AND artifacts.data IS NOT NULL "
                "  AND COALESCE((artifacts.data ->> 'is_proxy')::boolean, FALSE) = TRUE "
                "  AND jobs.is_proxy = FALSE"
            )
        )
        # FK indexes — Postgres doesn't auto-create them, and these columns
        # are the join keys for every per-contract batch prefetch.
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_upgrade_events_contract_id ON upgrade_events (contract_id)"))
        for table in (
            "privileged_functions",
            "role_definitions",
            "controller_values",
            "control_graph_nodes",
            "control_graph_edges",
            "effective_functions",
            "principal_labels",
            "contract_dependencies",
        ):
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{table}_contract_id ON {table} (contract_id)"))
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_function_principals_function_id ON function_principals (function_id)")
        )
        # audit_contract_coverage ↔ proxy invariant. Scope names like
        # ``UUPSProxy`` match generic proxy Contract rows verbatim, but the
        # audit didn't review the proxy's code — it reviewed the impl's.
        # ``services.audits.coverage`` filters proxies out at the candidate
        # query; this trigger is the data-layer enforcement so a raw SQL
        # INSERT (migration, manual backfill, a future bug) cannot create
        # false-positive coverage rows. CREATE OR REPLACE on the function
        # makes it idempotent across repeated migration runs.
        conn.execute(
            text(
                """
                CREATE OR REPLACE FUNCTION _reject_proxy_coverage() RETURNS TRIGGER AS $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM contracts WHERE id = NEW.contract_id AND is_proxy = TRUE) THEN
                        RAISE EXCEPTION
                            'audit_contract_coverage.contract_id=% references a proxy contract; '
                            'coverage rows must target implementations (is_proxy=FALSE)',
                            NEW.contract_id;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """
            )
        )
        # DROP + CREATE the trigger so a schema change to the function
        # propagates on the next migration run. DROP IF EXISTS keeps this
        # idempotent.
        conn.execute(text("DROP TRIGGER IF EXISTS audit_contract_coverage_no_proxy ON audit_contract_coverage"))
        conn.execute(
            text(
                "CREATE TRIGGER audit_contract_coverage_no_proxy "
                "BEFORE INSERT OR UPDATE ON audit_contract_coverage "
                "FOR EACH ROW EXECUTE FUNCTION _reject_proxy_coverage()"
            )
        )
        # One-shot cleanup of stale proxy coverage rows that predate the
        # trigger. DELETE doesn't fire the trigger (only INSERT/UPDATE
        # do), so the cleanup runs even when the trigger is already
        # armed. Idempotent: a no-op on subsequent migration runs
        # because the trigger prevents regression.
        conn.execute(
            text(
                "DELETE FROM audit_contract_coverage "
                "WHERE contract_id IN (SELECT id FROM contracts WHERE is_proxy = TRUE)"
            )
        )
        conn.commit()


def create_tables() -> None:
    """Create all tables (idempotent)."""
    # Ensure enum types exist before creating tables
    from sqlalchemy import text

    with engine.connect() as conn:
        for enum_val in ("queued", "processing", "completed", "failed"):
            conn.execute(
                text(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'jobstatus') "
                    "THEN CREATE TYPE jobstatus AS ENUM ('queued','processing','completed','failed'); END IF; END $$;"
                )
            )
        for enum_val in (
            "discovery",
            "dapp_crawl",
            "defillama_scan",
            "selection",
            "static",
            "resolution",
            "policy",
            "coverage",
            "done",
        ):
            conn.execute(
                text(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'jobstage') "
                    "THEN CREATE TYPE jobstage AS ENUM ("
                    "'discovery','dapp_crawl','defillama_scan','selection',"
                    "'static','resolution','policy','coverage','done'"
                    "); END IF; END $$;"
                )
            )
        conn.commit()
    Base.metadata.create_all(engine)
    apply_storage_migrations(engine)


def drop_tables() -> None:
    """Drop all tables. Use with caution."""
    Base.metadata.drop_all(engine)
