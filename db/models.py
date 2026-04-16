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
    protocol_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("protocols.id", ondelete="SET NULL"), nullable=True
    )
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
    protocol_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("protocols.id", ondelete="CASCADE"), nullable=False
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    pdf_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    auditor: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    date: Mapped[str | None] = mapped_column(String(20), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Numeric(5, 4), nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("protocol_id", "url", name="uq_audit_report_protocol_url"),
        Index("ix_audit_reports_protocol_id", "protocol_id"),
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
    discovery_source: Mapped[str | None] = mapped_column(String(100), nullable=True)
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


class RoleDefinition(Base):
    __tablename__ = "role_definitions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    contract_id: Mapped[int] = mapped_column(Integer, ForeignKey("contracts.id", ondelete="CASCADE"), nullable=False)
    role_name: Mapped[str] = mapped_column(String(255), nullable=False)
    declared_in: Mapped[str | None] = mapped_column(String(255), nullable=True)

    contract: Mapped[Contract] = relationship("Contract", back_populates="role_definitions")


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


class UpgradeEvent(Base):
    __tablename__ = "upgrade_events"

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

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, class_=Session, expire_on_commit=False)


def apply_storage_migrations(target_engine=None) -> None:
    """Add object-storage columns to existing artifact and source_files tables.

    Idempotent — every ALTER uses IF NOT EXISTS or is a NULL-relaxing change.
    Called from create_tables() during app startup and from test fixtures so
    a TEST_DATABASE_URL that already has the old schema picks up the new columns.
    """
    from sqlalchemy import text

    target = target_engine if target_engine is not None else engine
    with target.connect() as conn:
        conn.execute(text("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS storage_key VARCHAR(512)"))
        conn.execute(text("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS size_bytes BIGINT"))
        conn.execute(text("ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS content_type VARCHAR(64)"))
        conn.execute(text("ALTER TABLE source_files ADD COLUMN IF NOT EXISTS storage_key VARCHAR(512)"))
        conn.execute(text("ALTER TABLE source_files ALTER COLUMN content DROP NOT NULL"))
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
        for enum_val in ("discovery", "dapp_crawl", "defillama_scan", "static", "resolution", "policy", "done"):
            conn.execute(
                text(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'jobstage') "
                    "THEN CREATE TYPE jobstage AS ENUM ("
                    "'discovery','dapp_crawl','defillama_scan','static','resolution','policy','done'"
                    "); END IF; END $$;"
                )
            )
        conn.commit()
    Base.metadata.create_all(engine)
    apply_storage_migrations(engine)


def drop_tables() -> None:
    """Drop all tables. Use with caution."""
    Base.metadata.drop_all(engine)
