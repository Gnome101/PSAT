import json
import os
import sys
from pathlib import Path

import psycopg2
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.db_manager import DatabaseManager

# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _db_params() -> dict:
    return {
        "dbname":   "psat_test",
        "user":     "postgres",
        "password": "postgres",
        "host":     "localhost",
        "port":     "5432",
    }


def _is_db_available() -> bool:
    try:
        conn = psycopg2.connect(**_db_params())
        conn.close()
        print("DB CONNECTED")
        return True
    except psycopg2.OperationalError:
        print("DB FAIL")
        return False


# Applied at module level so every test is skipped when Postgres is absent.
pytestmark = pytest.mark.skipif(
    not _is_db_available(),
    reason="PostgreSQL not reachable — set TEST_DB_* env vars if needed.",
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def db_params() -> dict:
    return _db_params()


@pytest.fixture(scope="session")
def initialized_db(db_params) -> DatabaseManager:
    """Create schema once for the whole test session."""
    db = DatabaseManager(**db_params)
    db.initialize()
    yield db
    db.close()


# Truncation order respects FK constraints (children before parents).
_TRUNCATE_ORDER = [
    "claim_evidence",
    "finding_evidence",
    "claims",
    "findings",
    "evidence",
    "document",
    "source",
    "contract",
    "protocol",
]


@pytest.fixture(autouse=True)
def clean_db(initialized_db: DatabaseManager):
    """Truncate all tables before every test."""
    initialized_db.conn.rollback()
    with initialized_db._cursor() as cur:
        for table in _TRUNCATE_ORDER:
            cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')
    initialized_db.conn.commit()
    yield


@pytest.fixture()
def db(initialized_db: DatabaseManager) -> DatabaseManager:
    """Convenience alias used by individual tests."""
    return initialized_db


# ---------------------------------------------------------------------------
# Shared factory helpers
# ---------------------------------------------------------------------------

def make_protocol(db: DatabaseManager, name: str = "TestProtocol", chains=None) -> dict:
    data: dict = {"name": name}
    if chains is not None:
        data["chains"] = chains
    return db.insert("protocol", data)


def make_evidence(db: DatabaseManager, ref: str = "tx/0xabc") -> dict:
    return db.insert("evidence", {"reference": ref, "type": "test"})


def make_source(db: DatabaseManager, protocol_id: int) -> dict:
    return db.insert("source", {"protocol_id": protocol_id, "type": "rpc", "url": "https://example.com"})


def make_document(db: DatabaseManager, source_id: int) -> dict:
    return db.insert("document", {"source_id": source_id, "format": "evm_bytecode"})


def make_finding(db: DatabaseManager, protocol_id: int, title: str = "Test Finding") -> dict:
    return db.insert("findings", {"protocol_id": protocol_id, "title": title, "severity": "high"})


def make_claim(db: DatabaseManager, document_id: int) -> dict:
    return db.insert("claims", {"document_id": document_id, "category": "static_dependency", "value": "test"})


# ===========================================================================
# Initialisation
# ===========================================================================

class TestInitialize:
    def test_idempotent(self, db):
        """Calling initialize() twice must not raise."""
        db.initialize()  # second call — schema already exists

    def test_all_tables_exist(self, db):
        rows = db.execute_raw(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        existing = {r["tablename"] for r in rows}
        expected = {
            "protocol", "contract", "source", "document",
            "findings", "claims", "evidence",
            "finding_evidence", "claim_evidence",
        }
        assert expected.issubset(existing)


# ===========================================================================
# Table validation
# ===========================================================================

class TestValidateTable:
    def test_unknown_table_raises(self, db):
        with pytest.raises(ValueError, match="Unknown table"):
            db.insert("nonexistent_table", {"foo": "bar"})

    def test_known_tables_accepted(self, db):
        """No exception should be raised for every table in VALID_TABLES."""
        from db.db_manager import VALID_TABLES
        for table in VALID_TABLES:
            DatabaseManager._validate_table(table)  # must not raise


# ===========================================================================
# INSERT
# ===========================================================================

class TestInsert:
    def test_insert_returns_row_with_id(self, db):
        row = make_protocol(db)
        assert "id" in row
        assert row["id"] >= 1
        assert row["name"] == "TestProtocol"

    def test_insert_with_chains_array(self, db):
        row = make_protocol(db, chains=["ethereum", "arbitrum"])
        assert set(row["chains"]) == {"ethereum", "arbitrum"}

    def test_insert_empty_data_raises(self, db):
        with pytest.raises(ValueError, match="must not be empty"):
            db.insert("protocol", {})

    def test_insert_increments_id(self, db):
        r1 = make_protocol(db, "P1")
        r2 = make_protocol(db, "P2")
        assert r2["id"] > r1["id"]


# ===========================================================================
# BULK INSERT
# ===========================================================================

class TestBulkInsert:
    def test_bulk_insert_returns_count(self, db):
        rows = [{"name": f"Protocol {i}"} for i in range(5)]
        count = db.bulk_insert("protocol", rows)
        assert count == 5

    def test_bulk_insert_empty_list(self, db):
        assert db.bulk_insert("protocol", []) == 0

    def test_bulk_insert_data_present(self, db):
        rows = [{"name": "BulkA"}, {"name": "BulkB"}]
        db.bulk_insert("protocol", rows)
        all_rows = db.get_all("protocol")
        names = {r["name"] for r in all_rows}
        assert {"BulkA", "BulkB"}.issubset(names)


# ===========================================================================
# GET BY ID
# ===========================================================================

class TestGetById:
    def test_get_existing(self, db):
        inserted = make_protocol(db, "Proto")
        fetched = db.get_by_id("protocol", inserted["id"])
        assert fetched is not None
        assert fetched["name"] == "Proto"

    def test_get_missing_returns_none(self, db):
        assert db.get_by_id("protocol", 99999) is None


# ===========================================================================
# GET ALL
# ===========================================================================

class TestGetAll:
    def test_get_all_returns_all_rows(self, db):
        make_protocol(db, "A")
        make_protocol(db, "B")
        make_protocol(db, "C")
        rows = db.get_all("protocol")
        assert len(rows) == 3

    def test_get_all_empty_table(self, db):
        assert db.get_all("protocol") == []

    def test_get_all_where_filter(self, db):
        p = make_protocol(db, "Target")
        make_protocol(db, "Other")
        rows = db.get_all("protocol", where={"name": "Target"})
        assert len(rows) == 1
        assert rows[0]["id"] == p["id"]

    def test_get_all_order_by_asc(self, db):
        make_protocol(db, "Zebra")
        make_protocol(db, "Alpha")
        rows = db.get_all("protocol", order_by="name")
        assert rows[0]["name"] == "Alpha"

    def test_get_all_order_by_desc(self, db):
        make_protocol(db, "Zebra")
        make_protocol(db, "Alpha")
        rows = db.get_all("protocol", order_by="-name")
        assert rows[0]["name"] == "Zebra"

    def test_get_all_limit(self, db):
        for i in range(5):
            make_protocol(db, f"P{i}")
        rows = db.get_all("protocol", limit=3)
        assert len(rows) == 3

    def test_get_all_offset(self, db):
        for i in range(4):
            make_protocol(db, f"P{i}")
        all_rows = db.get_all("protocol", order_by="id")
        paged   = db.get_all("protocol", order_by="id", limit=2, offset=2)
        assert paged[0]["id"] == all_rows[2]["id"]


# ===========================================================================
# SEARCH
# ===========================================================================

class TestSearch:
    def test_search_case_insensitive(self, db):
        make_protocol(db, "UniswapV3")
        make_protocol(db, "AaveProtocol")
        results = db.search("protocol", "name", "UNISWAP")
        assert len(results) == 1
        assert results[0]["name"] == "UniswapV3"

    def test_search_no_results(self, db):
        make_protocol(db, "Compound")
        assert db.search("protocol", "name", "xyz_no_match") == []

    def test_search_partial_match(self, db):
        make_protocol(db, "CurveFinance")
        make_protocol(db, "CurveDAO")
        results = db.search("protocol", "name", "curve")
        assert len(results) == 2


# ===========================================================================
# COUNT
# ===========================================================================

class TestCount:
    def test_count_empty(self, db):
        assert db.count("protocol") == 0

    def test_count_with_rows(self, db):
        make_protocol(db, "A")
        make_protocol(db, "B")
        assert db.count("protocol") == 2

    def test_count_with_where(self, db):
        make_protocol(db, "Target")
        make_protocol(db, "Other")
        assert db.count("protocol", where={"name": "Target"}) == 1


# ===========================================================================
# UPDATE
# ===========================================================================

class TestUpdate:
    def test_update_returns_updated_row(self, db):
        p = make_protocol(db, "OldName")
        updated = db.update("protocol", p["id"], {"name": "NewName"})
        assert updated is not None
        assert updated["name"] == "NewName"

    def test_update_persisted(self, db):
        p = make_protocol(db, "Before")
        db.update("protocol", p["id"], {"name": "After"})
        fetched = db.get_by_id("protocol", p["id"])
        assert fetched["name"] == "After"

    def test_update_missing_returns_none(self, db):
        assert db.update("protocol", 99999, {"name": "Ghost"}) is None

    def test_update_empty_data_raises(self, db):
        p = make_protocol(db)
        with pytest.raises(ValueError, match="must not be empty"):
            db.update("protocol", p["id"], {})

    def test_update_chains_array(self, db):
        p = make_protocol(db, chains=["ethereum"])
        updated = db.update("protocol", p["id"], {"chains": ["ethereum", "base"]})
        assert set(updated["chains"]) == {"ethereum", "base"}


# ===========================================================================
# DELETE
# ===========================================================================

class TestDelete:
    def test_delete_existing(self, db):
        p = make_protocol(db)
        assert db.delete("protocol", p["id"]) is True
        assert db.get_by_id("protocol", p["id"]) is None

    def test_delete_missing_returns_false(self, db):
        assert db.delete("protocol", 99999) is False

    def test_delete_cascades_to_contracts(self, db):
        p = make_protocol(db)
        db.insert("contract", {
            "protocol_id": p["id"], "address": "0xabc", "chain": "ethereum"
        })
        db.delete("protocol", p["id"])
        contracts = db.get_all("contract", where={"protocol_id": p["id"]})
        assert contracts == []


# ===========================================================================
# Chain helpers
# ===========================================================================

class TestChainHelpers:
    def test_add_chain_to_empty(self, db):
        p = make_protocol(db)
        updated = db.add_chain(p["id"], "ethereum")
        assert "ethereum" in updated["chains"]

    def test_add_chain_deduplicates(self, db):
        p = make_protocol(db, chains=["ethereum"])
        db.add_chain(p["id"], "ethereum")
        fetched = db.get_by_id("protocol", p["id"])
        assert fetched["chains"].count("ethereum") == 1

    def test_add_multiple_chains(self, db):
        p = make_protocol(db)
        db.add_chain(p["id"], "ethereum")
        db.add_chain(p["id"], "arbitrum")
        db.add_chain(p["id"], "base")
        fetched = db.get_by_id("protocol", p["id"])
        assert set(fetched["chains"]) == {"ethereum", "arbitrum", "base"}

    def test_add_chain_missing_protocol_returns_none(self, db):
        assert db.add_chain(99999, "ethereum") is None

    def test_remove_chain(self, db):
        p = make_protocol(db, chains=["ethereum", "arbitrum"])
        updated = db.remove_chain(p["id"], "arbitrum")
        assert "arbitrum" not in updated["chains"]
        assert "ethereum" in updated["chains"]

    def test_remove_chain_not_present_is_noop(self, db):
        p = make_protocol(db, chains=["ethereum"])
        updated = db.remove_chain(p["id"], "optimism")
        assert updated["chains"] == ["ethereum"]

    def test_remove_chain_missing_protocol_returns_none(self, db):
        assert db.remove_chain(99999, "ethereum") is None

    def test_get_protocols_by_chain_single_match(self, db):
        p = make_protocol(db, "OnEth", chains=["ethereum"])
        make_protocol(db, "OnBase", chains=["base"])
        results = db.get_protocols_by_chain("ethereum")
        ids = [r["id"] for r in results]
        assert p["id"] in ids
        assert all(r["id"] != make_protocol(db, "Dummy")["id"] for r in results if r["name"] == "OnBase")

    def test_get_protocols_by_chain_multi_chain_protocol(self, db):
        p = make_protocol(db, "Multi", chains=["ethereum", "arbitrum"])
        assert any(r["id"] == p["id"] for r in db.get_protocols_by_chain("ethereum"))
        assert any(r["id"] == p["id"] for r in db.get_protocols_by_chain("arbitrum"))

    def test_get_protocols_by_chain_no_match(self, db):
        make_protocol(db, chains=["ethereum"])
        assert db.get_protocols_by_chain("solana") == []

    def test_get_protocols_by_chains_any(self, db):
        p1 = make_protocol(db, "P1", chains=["ethereum"])
        p2 = make_protocol(db, "P2", chains=["base"])
        make_protocol(db, "P3", chains=["polygon"])
        results = db.get_protocols_by_chains(["ethereum", "base"], match_all=False)
        result_ids = {r["id"] for r in results}
        assert p1["id"] in result_ids
        assert p2["id"] in result_ids

    def test_get_protocols_by_chains_all(self, db):
        p_both = make_protocol(db, "Both", chains=["ethereum", "arbitrum"])
        make_protocol(db, "OnlyEth", chains=["ethereum"])
        results = db.get_protocols_by_chains(["ethereum", "arbitrum"], match_all=True)
        result_ids = {r["id"] for r in results}
        assert p_both["id"] in result_ids
        # protocol with only ethereum must NOT appear
        only_eth_ids = {r["id"] for r in db.get_protocols_by_chain("ethereum")
                        if r["name"] == "OnlyEth"}
        assert result_ids.isdisjoint(only_eth_ids)

    def test_chains_sorted_alphabetically(self, db):
        p = make_protocol(db)
        for chain in ["polygon", "arbitrum", "ethereum", "base"]:
            db.add_chain(p["id"], chain)
        fetched = db.get_by_id("protocol", p["id"])
        assert fetched["chains"] == sorted(fetched["chains"])


# ===========================================================================
# Finding ↔ Evidence junction
# ===========================================================================

class TestFindingEvidence:
    @pytest.fixture()
    def setup(self, db):
        p   = make_protocol(db)
        ev  = make_evidence(db)
        f   = make_finding(db, p["id"])
        return {"protocol": p, "evidence": ev, "finding": f}

    def test_link_finding_evidence(self, db, setup):
        row = db.link_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        assert row["finding_id"] == setup["finding"]["id"]
        assert row["evidence_id"] == setup["evidence"]["id"]

    def test_get_evidence_for_finding(self, db, setup):
        db.link_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        evidences = db.get_evidence_for_finding(setup["finding"]["id"])
        assert len(evidences) == 1
        assert evidences[0]["id"] == setup["evidence"]["id"]

    def test_get_evidence_for_finding_empty(self, db, setup):
        assert db.get_evidence_for_finding(setup["finding"]["id"]) == []

    def test_unlink_finding_evidence(self, db, setup):
        db.link_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        removed = db.unlink_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        assert removed is True
        assert db.get_evidence_for_finding(setup["finding"]["id"]) == []

    def test_unlink_finding_evidence_missing(self, db, setup):
        assert db.unlink_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"]) is False

    def test_duplicate_link_raises(self, db, setup):
        db.link_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        with pytest.raises(Exception):
            db.link_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        db.conn.rollback()

    def test_multiple_evidence_for_finding(self, db, setup):
        ev2 = make_evidence(db, "tx/0xdef")
        db.link_finding_evidence(setup["finding"]["id"], setup["evidence"]["id"])
        db.link_finding_evidence(setup["finding"]["id"], ev2["id"])
        assert len(db.get_evidence_for_finding(setup["finding"]["id"])) == 2


# ===========================================================================
# Claim ↔ Evidence junction
# ===========================================================================

class TestClaimEvidence:
    @pytest.fixture()
    def setup(self, db):
        p    = make_protocol(db)
        src  = make_source(db, p["id"])
        doc  = make_document(db, src["id"])
        ev   = make_evidence(db)
        cl   = make_claim(db, doc["id"])
        return {"protocol": p, "source": src, "document": doc, "evidence": ev, "claim": cl}

    def test_link_claim_evidence(self, db, setup):
        row = db.link_claim_evidence(setup["claim"]["id"], setup["evidence"]["id"])
        assert row["claim_id"] == setup["claim"]["id"]
        assert row["evidence_id"] == setup["evidence"]["id"]

    def test_get_evidence_for_claim(self, db, setup):
        db.link_claim_evidence(setup["claim"]["id"], setup["evidence"]["id"])
        evidences = db.get_evidence_for_claim(setup["claim"]["id"])
        assert len(evidences) == 1
        assert evidences[0]["id"] == setup["evidence"]["id"]

    def test_get_evidence_for_claim_empty(self, db, setup):
        assert db.get_evidence_for_claim(setup["claim"]["id"]) == []

    def test_unlink_claim_evidence(self, db, setup):
        db.link_claim_evidence(setup["claim"]["id"], setup["evidence"]["id"])
        removed = db.unlink_claim_evidence(setup["claim"]["id"], setup["evidence"]["id"])
        assert removed is True
        assert db.get_evidence_for_claim(setup["claim"]["id"]) == []

    def test_unlink_claim_evidence_missing(self, db, setup):
        assert db.unlink_claim_evidence(setup["claim"]["id"], setup["evidence"]["id"]) is False


# ===========================================================================
# execute_raw
# ===========================================================================

class TestExecuteRaw:
    def test_raw_select(self, db):
        make_protocol(db, "RawTest")
        rows = db.execute_raw("SELECT name FROM protocol WHERE name = %s", ("RawTest",))
        assert len(rows) == 1
        assert rows[0]["name"] == "RawTest"

    def test_raw_select_no_results(self, db):
        assert db.execute_raw("SELECT * FROM protocol WHERE id = %s", (99999,)) == []

    def test_raw_count(self, db):
        make_protocol(db, "A")
        make_protocol(db, "B")
        rows = db.execute_raw("SELECT COUNT(*) AS total FROM protocol")
        assert rows[0]["total"] == 2


# ===========================================================================
# Context manager
# ===========================================================================

class TestContextManager:
    def test_context_manager_connects_and_closes(self, db_params):
        with DatabaseManager(**db_params) as db:
            assert db.conn is not None
            assert not db.conn.closed
            make_protocol(db)
        assert db.conn.closed

    def test_context_manager_closes_on_exception(self, db_params):
        """Connection must be closed even when the body raises."""
        captured_db = None
        try:
            with DatabaseManager(**db_params) as db:
                captured_db = db
                db.insert("protocol", {"name": "ContextExceptionTest"})
                raise RuntimeError("forced failure")
        except RuntimeError:
            pass
 
        assert captured_db is not None
        assert captured_db.conn.closed


# ===========================================================================
# repr
# ===========================================================================

class TestRepr:
    def test_repr_disconnected(self, db_params):
        db = DatabaseManager(**db_params)
        assert "disconnected" in repr(db)

    def test_repr_connected(self, db_params):
        db = DatabaseManager(**db_params)
        db.connect()
        assert "connected" in repr(db)
        db.close()


# ===========================================================================
# Cascade / referential integrity
# ===========================================================================

class TestReferentialIntegrity:
    def test_insert_contract_requires_valid_protocol(self, db):
        with pytest.raises(Exception):
            db.insert("contract", {
                "protocol_id": 99999,
                "address": "0xdead",
                "chain": "ethereum",
            })
        db.conn.rollback()

    def test_insert_source_requires_valid_protocol(self, db):
        with pytest.raises(Exception):
            db.insert("source", {"protocol_id": 99999, "type": "rpc"})
        db.conn.rollback()

    def test_insert_document_requires_valid_source(self, db):
        with pytest.raises(Exception):
            db.insert("document", {"source_id": 99999, "format": "evm_bytecode"})
        db.conn.rollback()

    def test_insert_finding_requires_valid_protocol(self, db):
        with pytest.raises(Exception):
            db.insert("findings", {"protocol_id": 99999, "title": "Bad"})
        db.conn.rollback()

    def test_insert_claim_requires_valid_document(self, db):
        with pytest.raises(Exception):
            db.insert("claims", {"document_id": 99999, "category": "x", "value": "y"})
        db.conn.rollback()