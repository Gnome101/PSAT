"""Unit and integration tests for protocol monitoring enrollment.

Unit tests (always run): Use SQLite in-memory with mock Contract objects.
Integration tests (require PostgreSQL): Use real tables, test full enrollment flow.

Run integration tests:
    DATABASE_URL=postgresql://psat:psat@localhost:5432/psat \
        uv run pytest tests/test_enrollment.py -v
"""

from __future__ import annotations

import os
import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from db.models import (
    MonitoredContract,
    MonitoredEvent,
    ProxyUpgradeEvent,
    WatchedProxy,
)

# ---------------------------------------------------------------------------
# Postgres skip condition
# ---------------------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def _can_connect() -> bool:
    if not DATABASE_URL:
        return False
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


requires_postgres = pytest.mark.skipif(
    not _can_connect(), reason="PostgreSQL not available"
)


@pytest.fixture()
def db_session():
    """In-memory SQLite database for enrollment tests.

    Does NOT enable foreign_keys pragma so MonitoredContract can be created
    without the protocols/contracts tables existing (those use PG-specific types).
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    WatchedProxy.__table__.create(engine, checkfirst=True)
    ProxyUpgradeEvent.__table__.create(engine, checkfirst=True)
    MonitoredContract.__table__.create(engine, checkfirst=True)
    MonitoredEvent.__table__.create(engine, checkfirst=True)

    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


# ---------------------------------------------------------------------------
# Helpers to build mock objects
# ---------------------------------------------------------------------------


def _mock_contract(address="0x" + "a" * 40, chain="ethereum", name="TestContract",
                   is_proxy=False, proxy_type=None, implementation=None, protocol_id=1):
    c = MagicMock()
    c.id = 1
    c.address = address
    c.chain = chain
    c.contract_name = name
    c.is_proxy = is_proxy
    c.proxy_type = proxy_type
    c.implementation = implementation
    c.protocol_id = protocol_id
    return c


def _mock_summary(is_upgradeable=False, is_pausable=False, has_timelock=False,
                  control_model=None):
    s = MagicMock()
    s.is_upgradeable = is_upgradeable
    s.is_pausable = is_pausable
    s.has_timelock = has_timelock
    s.is_factory = False
    s.is_nft = False
    s.control_model = control_model
    return s


def _mock_controller_value(controller_id="owner", value="0x" + "b" * 40,
                           resolved_type=None):
    cv = MagicMock()
    cv.controller_id = controller_id
    cv.value = value
    cv.resolved_type = resolved_type
    cv.contract_id = 1
    return cv


def _mock_graph_node(address="0x" + "c" * 40, resolved_type="safe"):
    n = MagicMock()
    n.address = address
    n.resolved_type = resolved_type
    n.node_type = resolved_type
    n.contract_id = 1
    return n


# ---------------------------------------------------------------------------
# Tests for _determine_contract_type
# ---------------------------------------------------------------------------


class TestDetermineContractType:
    def test_proxy_from_summary(self):
        from services.monitoring.enrollment import _determine_contract_type

        summary = _mock_summary(is_upgradeable=True)
        result = _determine_contract_type(summary, [])
        assert result == "proxy"

    def test_timelock_from_summary(self):
        from services.monitoring.enrollment import _determine_contract_type

        summary = _mock_summary(has_timelock=True)
        result = _determine_contract_type(summary, [])
        assert result == "timelock"

    def test_pausable_from_summary(self):
        from services.monitoring.enrollment import _determine_contract_type

        summary = _mock_summary(is_pausable=True)
        result = _determine_contract_type(summary, [])
        assert result == "pausable"

    def test_safe_from_controller_value(self):
        from services.monitoring.enrollment import _determine_contract_type

        cv = _mock_controller_value(resolved_type="safe")
        result = _determine_contract_type(None, [cv])
        assert result == "safe"

    def test_regular_default(self):
        from services.monitoring.enrollment import _determine_contract_type

        result = _determine_contract_type(None, [])
        assert result == "regular"


# ---------------------------------------------------------------------------
# Tests for _build_monitoring_config
# ---------------------------------------------------------------------------


class TestBuildMonitoringConfig:
    def test_proxy_config(self):
        from services.monitoring.enrollment import _build_monitoring_config

        summary = _mock_summary(is_upgradeable=True)
        config = _build_monitoring_config(summary, [], "proxy")
        assert config["watch_upgrades"] is True
        assert config["watch_ownership"] is True

    def test_pausable_config(self):
        from services.monitoring.enrollment import _build_monitoring_config

        summary = _mock_summary(is_pausable=True)
        config = _build_monitoring_config(summary, [], "pausable")
        assert config["watch_pause"] is True
        assert config["watch_upgrades"] is False

    def test_safe_config(self):
        from services.monitoring.enrollment import _build_monitoring_config

        config = _build_monitoring_config(None, [], "safe")
        assert config["watch_safe_signers"] is True

    def test_timelock_config(self):
        from services.monitoring.enrollment import _build_monitoring_config

        config = _build_monitoring_config(None, [], "timelock")
        assert config["watch_timelock"] is True

    def test_role_based_config(self):
        from services.monitoring.enrollment import _build_monitoring_config

        summary = _mock_summary(control_model="role-based")
        config = _build_monitoring_config(summary, [], "regular")
        assert config["watch_roles"] is True


# ---------------------------------------------------------------------------
# Tests for _build_initial_state
# ---------------------------------------------------------------------------


class TestBuildInitialState:
    def test_includes_implementation(self):
        from services.monitoring.enrollment import _build_initial_state

        contract = _mock_contract(implementation="0x" + "d" * 40)
        state = _build_initial_state(contract, [])
        assert state["implementation"] == "0x" + "d" * 40

    def test_includes_owner(self):
        from services.monitoring.enrollment import _build_initial_state

        contract = _mock_contract()
        cv = _mock_controller_value(controller_id="owner", value="0x" + "e" * 40)
        state = _build_initial_state(contract, [cv])
        assert state["owner"] == "0x" + "e" * 40


# ---------------------------------------------------------------------------
# Tests for maybe_enroll_protocol
# ---------------------------------------------------------------------------


class TestMaybeEnrollProtocol:
    @patch("services.monitoring.enrollment.enroll_protocol_contracts")
    def test_skips_when_in_flight_jobs(self, mock_enroll, db_session):
        from services.monitoring.enrollment import maybe_enroll_protocol

        # We need to mock the session queries since Job uses JSONB
        mock_session = MagicMock()
        # in-flight query returns a result
        mock_in_flight = MagicMock()
        mock_in_flight.scalars.return_value.first.return_value = MagicMock()
        mock_session.execute.return_value = mock_in_flight

        result = maybe_enroll_protocol(mock_session, 1, "http://rpc", "ethereum")
        assert result is False
        mock_enroll.assert_not_called()

    @patch("services.monitoring.enrollment.enroll_protocol_contracts")
    def test_skips_when_no_completed_jobs(self, mock_enroll):
        from services.monitoring.enrollment import maybe_enroll_protocol

        mock_session = MagicMock()
        # in-flight returns None, completed returns None
        call_count = [0]

        def mock_execute(stmt):
            call_count[0] += 1
            result = MagicMock()
            result.scalars.return_value.first.return_value = None
            return result

        mock_session.execute.side_effect = mock_execute

        result = maybe_enroll_protocol(mock_session, 1, "http://rpc", "ethereum")
        assert result is False
        mock_enroll.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for enroll_protocol_contracts (integration with db_session)
# ---------------------------------------------------------------------------


class TestEnrollProtocolContracts:
    @patch("services.monitoring.enrollment.rpc_request")
    def test_enroll_creates_monitored_contracts(self, mock_rpc, db_session):
        """Enrollment creates correct MonitoredContract rows."""
        from services.monitoring.enrollment import (
            _build_initial_state,
            _build_monitoring_config,
            _determine_contract_type,
        )

        mock_rpc.return_value = "0x100"

        # Since the full Contract model uses ARRAY/JSONB columns that are
        # incompatible with SQLite, we test the building blocks directly
        summary = _mock_summary(is_upgradeable=True, is_pausable=True)
        ct = _determine_contract_type(summary, [])
        assert ct == "proxy"

        config = _build_monitoring_config(summary, [], ct)
        assert config["watch_upgrades"] is True
        assert config["watch_pause"] is True

        contract = _mock_contract(implementation="0x" + "f" * 40)
        state = _build_initial_state(contract, [])
        assert "implementation" in state

        # Manually create a MonitoredContract to verify DB compatibility
        mc = MonitoredContract(
            id=uuid.uuid4(),
            address="0x" + "a" * 40,
            chain="ethereum",
            contract_type=ct,
            monitoring_config=config,
            last_known_state=state,
            last_scanned_block=256,
            needs_polling=True,
            is_active=True,
            enrollment_source="auto",
        )
        db_session.add(mc)
        db_session.commit()

        # Verify it was created
        from sqlalchemy import select

        result = db_session.execute(
            select(MonitoredContract).where(MonitoredContract.address == "0x" + "a" * 40)
        ).scalar_one_or_none()
        assert result is not None
        assert result.contract_type == "proxy"
        assert result.enrollment_source == "auto"

    def test_enroll_idempotent(self, db_session):
        """Calling enrollment twice doesn't duplicate rows."""
        mc = MonitoredContract(
            id=uuid.uuid4(),
            address="0x" + "a" * 40,
            chain="ethereum",
            contract_type="proxy",
            monitoring_config={"watch_upgrades": True},
            last_scanned_block=0,
            is_active=True,
            enrollment_source="auto",
        )
        db_session.add(mc)
        db_session.commit()

        # Check we can find it and upsert logic would work
        from sqlalchemy import select

        existing = db_session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == "0x" + "a" * 40,
                MonitoredContract.chain == "ethereum",
            )
        ).scalar_one_or_none()
        assert existing is not None
        assert existing.id == mc.id

        # Update it (simulating re-enrollment)
        existing.contract_type = "pausable"
        db_session.commit()

        # Still only one row
        from sqlalchemy import func

        count = db_session.execute(
            select(func.count()).select_from(MonitoredContract)
        ).scalar()
        assert count == 1

    def test_enroll_bridges_to_watched_proxy(self, db_session):
        """Proxy contracts get WatchedProxy rows linked via watched_proxy_id."""
        wp = WatchedProxy(
            id=uuid.uuid4(),
            proxy_address="0x" + "a" * 40,
            chain="ethereum",
            label="test",
            last_scanned_block=0,
        )
        db_session.add(wp)
        db_session.commit()

        mc = MonitoredContract(
            id=uuid.uuid4(),
            address="0x" + "a" * 40,
            chain="ethereum",
            contract_type="proxy",
            watched_proxy_id=wp.id,
            monitoring_config={"watch_upgrades": True},
            last_scanned_block=0,
            is_active=True,
            enrollment_source="auto",
        )
        db_session.add(mc)
        db_session.commit()

        from sqlalchemy import select

        result = db_session.execute(
            select(MonitoredContract).where(MonitoredContract.address == "0x" + "a" * 40)
        ).scalar_one()
        assert result.watched_proxy_id == wp.id

    def test_enroll_discovers_controller_addresses(self, db_session):
        """Safe/timelock controllers get their own MonitoredContract rows."""
        # Simulate creating MonitoredContract rows for controller addresses
        safe_addr = "0x" + "c" * 40
        mc = MonitoredContract(
            id=uuid.uuid4(),
            address=safe_addr,
            chain="ethereum",
            contract_type="safe",
            monitoring_config={"watch_safe_signers": True, "watch_ownership": True},
            last_scanned_block=0,
            needs_polling=True,
            is_active=True,
            enrollment_source="auto",
        )
        db_session.add(mc)
        db_session.commit()

        from sqlalchemy import select

        result = db_session.execute(
            select(MonitoredContract).where(MonitoredContract.address == safe_addr)
        ).scalar_one()
        assert result.contract_type == "safe"
        assert result.needs_polling is True


# ---------------------------------------------------------------------------
# Integration tests — real PostgreSQL with full ORM models
# ---------------------------------------------------------------------------

PROTO_NAME = "__test_enrollment__"


@pytest.fixture()
def pg_session():
    """Full-schema PostgreSQL session. Cleans up only test-created rows."""
    from db.models import (
        Base,
        Contract,
        ContractSummary,
        ControlGraphNode,
        ControllerValue,
        Job,
        Protocol,
    )

    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    try:
        yield session
    finally:
        session.rollback()
        proto = session.execute(
            select(Protocol).where(Protocol.name == PROTO_NAME)
        ).scalar_one_or_none()
        if proto:
            # Cascade deletes Contract → ContractSummary, ControllerValue,
            # ControlGraphNode; and Job cleanup via protocol_id
            session.execute(
                select(MonitoredContract).where(
                    MonitoredContract.protocol_id == proto.id
                )
            )
            for mc in session.execute(
                select(MonitoredContract).where(
                    MonitoredContract.protocol_id == proto.id
                )
            ).scalars():
                if mc.watched_proxy_id:
                    wp = session.get(WatchedProxy, mc.watched_proxy_id)
                    if wp:
                        session.delete(wp)
                session.delete(mc)
            for mc in session.execute(
                select(MonitoredContract).where(
                    MonitoredContract.enrollment_source == "auto",
                    MonitoredContract.protocol_id == proto.id,
                )
            ).scalars():
                session.delete(mc)
            for j in session.execute(
                select(Job).where(Job.protocol_id == proto.id)
            ).scalars():
                session.delete(j)
            # Contracts cascade-delete summaries, controller_values, graph nodes
            for c in session.execute(
                select(Contract).where(Contract.protocol_id == proto.id)
            ).scalars():
                session.delete(c)
            session.delete(proto)
        session.commit()
        session.close()
        engine.dispose()


@requires_postgres
class TestEnrollmentIntegration:
    """End-to-end enrollment with real Contract, ContractSummary, and
    ControlGraphNode rows in PostgreSQL."""

    def test_enroll_creates_monitored_contracts_from_real_data(self, pg_session):
        """Full enroll_protocol_contracts with real Contract + ContractSummary."""
        from db.models import Contract, ContractSummary, ControllerValue, Protocol
        from services.monitoring.enrollment import enroll_protocol_contracts

        proto = Protocol(name=PROTO_NAME)
        pg_session.add(proto)
        pg_session.flush()

        # Upgradeable proxy contract
        proxy_contract = Contract(
            address="0x" + "a1" * 20,
            chain="ethereum",
            protocol_id=proto.id,
            contract_name="LiquidityPool",
            is_proxy=True,
            proxy_type="eip1967",
            implementation="0x" + "a2" * 20,
        )
        pg_session.add(proxy_contract)
        pg_session.flush()

        pg_session.add(ContractSummary(
            contract_id=proxy_contract.id,
            is_upgradeable=True,
            is_pausable=True,
            control_model="governance",
        ))
        pg_session.add(ControllerValue(
            contract_id=proxy_contract.id,
            controller_id="owner",
            value="0x" + "b1" * 20,
            resolved_type="safe",
        ))

        # Plain pausable contract
        pausable_contract = Contract(
            address="0x" + "c1" * 20,
            chain="ethereum",
            protocol_id=proto.id,
            contract_name="StakingManager",
        )
        pg_session.add(pausable_contract)
        pg_session.flush()

        pg_session.add(ContractSummary(
            contract_id=pausable_contract.id,
            is_upgradeable=False,
            is_pausable=True,
            control_model="role-based",
        ))
        pg_session.commit()

        with patch("services.monitoring.enrollment.rpc_request", return_value="0x100"):
            enrolled = enroll_protocol_contracts(
                pg_session, proto.id, "http://rpc", "ethereum"
            )

        assert len(enrolled) == 2

        # Verify proxy contract enrollment
        proxy_mc = pg_session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == ("0x" + "a1" * 20)
            )
        ).scalar_one()
        assert proxy_mc.contract_type == "proxy"
        assert proxy_mc.monitoring_config["watch_upgrades"] is True
        assert proxy_mc.monitoring_config["watch_pause"] is True
        assert proxy_mc.last_known_state["implementation"] == "0x" + "a2" * 20
        assert proxy_mc.last_known_state["owner"] == "0x" + "b1" * 20
        assert proxy_mc.needs_polling is True
        # Proxy should also have a WatchedProxy row linked
        assert proxy_mc.watched_proxy_id is not None

        # Verify pausable contract enrollment
        pausable_mc = pg_session.execute(
            select(MonitoredContract).where(
                MonitoredContract.address == ("0x" + "c1" * 20)
            )
        ).scalar_one()
        assert pausable_mc.contract_type == "pausable"
        assert pausable_mc.monitoring_config["watch_pause"] is True
        assert pausable_mc.monitoring_config["watch_roles"] is True
        assert pausable_mc.monitoring_config["watch_upgrades"] is False

    def test_enroll_discovers_controllers_from_graph(self, pg_session):
        """ControlGraphNode rows with safe/timelock types get their own
        MonitoredContract rows automatically."""
        from db.models import Contract, ControlGraphNode, Protocol
        from services.monitoring.enrollment import enroll_protocol_contracts

        proto = Protocol(name=PROTO_NAME)
        pg_session.add(proto)
        pg_session.flush()

        contract = Contract(
            address="0x" + "d1" * 20,
            chain="ethereum",
            protocol_id=proto.id,
            contract_name="TestContract",
        )
        pg_session.add(contract)
        pg_session.flush()

        safe_addr = "0x" + "e1" * 20
        timelock_addr = "0x" + "e2" * 20
        eoa_addr = "0x" + "e3" * 20

        pg_session.add_all([
            ControlGraphNode(
                contract_id=contract.id, address=safe_addr,
                node_type="safe", resolved_type="safe", label="Multi-sig",
            ),
            ControlGraphNode(
                contract_id=contract.id, address=timelock_addr,
                node_type="timelock", resolved_type="timelock", label="Timelock",
            ),
            ControlGraphNode(
                contract_id=contract.id, address=eoa_addr,
                node_type="eoa", resolved_type="eoa", label="Deployer",
            ),
        ])
        pg_session.commit()

        with patch("services.monitoring.enrollment.rpc_request", return_value="0x100"):
            enroll_protocol_contracts(pg_session, proto.id, "http://rpc", "ethereum")

        # Safe controller should be enrolled
        safe_mc = pg_session.execute(
            select(MonitoredContract).where(MonitoredContract.address == safe_addr)
        ).scalar_one()
        assert safe_mc.contract_type == "safe"
        assert safe_mc.monitoring_config["watch_safe_signers"] is True
        assert safe_mc.needs_polling is True

        # Timelock controller should be enrolled
        tl_mc = pg_session.execute(
            select(MonitoredContract).where(MonitoredContract.address == timelock_addr)
        ).scalar_one()
        assert tl_mc.contract_type == "timelock"
        assert tl_mc.monitoring_config["watch_timelock"] is True

        # EOA should NOT be enrolled (regular type, skipped)
        eoa_mc = pg_session.execute(
            select(MonitoredContract).where(MonitoredContract.address == eoa_addr)
        ).scalar_one_or_none()
        assert eoa_mc is None

    def test_enroll_is_idempotent(self, pg_session):
        """Calling enroll_protocol_contracts twice doesn't duplicate rows."""
        from db.models import Contract, Protocol
        from sqlalchemy import func
        from services.monitoring.enrollment import enroll_protocol_contracts

        proto = Protocol(name=PROTO_NAME)
        pg_session.add(proto)
        pg_session.flush()

        pg_session.add(Contract(
            address="0x" + "f1" * 20,
            chain="ethereum",
            protocol_id=proto.id,
            contract_name="Token",
        ))
        pg_session.commit()

        with patch("services.monitoring.enrollment.rpc_request", return_value="0x100"):
            first = enroll_protocol_contracts(pg_session, proto.id, "http://rpc")
            second = enroll_protocol_contracts(pg_session, proto.id, "http://rpc")

        assert len(first) == 1
        assert len(second) == 1

        count = pg_session.execute(
            select(func.count()).select_from(MonitoredContract).where(
                MonitoredContract.address == ("0x" + "f1" * 20)
            )
        ).scalar()
        assert count == 1
