#!/usr/bin/env python3
import psycopg2
from psycopg2 import sql, extras
from datetime import datetime, timezone
from typing import Any, Optional

# Schema Definition

SCHEMA_SQL = """
-- ============================================
--  TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS protocol (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    chains          VARCHAR(100)[] DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS contract (
    id              SERIAL PRIMARY KEY,
    protocol_id     INTEGER NOT NULL REFERENCES protocol(id) ON DELETE CASCADE,
    address         VARCHAR(255) NOT NULL,
    chain           VARCHAR(100),
    is_proxy        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS source (
    id              SERIAL PRIMARY KEY,
    protocol_id     INTEGER NOT NULL REFERENCES protocol(id) ON DELETE CASCADE,
    type            VARCHAR(100),
    url             TEXT,
    authority_score NUMERIC(5, 2),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS document (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER NOT NULL REFERENCES source(id) ON DELETE CASCADE,
    format          VARCHAR(50),
    content_hash    VARCHAR(255),
    storage_path    TEXT,
    fetched_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS findings (
    id              SERIAL PRIMARY KEY,
    protocol_id     INTEGER NOT NULL REFERENCES protocol(id) ON DELETE CASCADE,
    title           VARCHAR(255),
    description     TEXT,
    severity        VARCHAR(50),
    status          VARCHAR(50) DEFAULT 'open',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS claims (
    id              SERIAL PRIMARY KEY,
    document_id     INTEGER NOT NULL REFERENCES document(id) ON DELETE CASCADE,
    category        VARCHAR(100),
    value           TEXT,
    confidence      NUMERIC(5, 4),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS evidence (
    id              SERIAL PRIMARY KEY,
    reference       TEXT,
    type            VARCHAR(100),
    checksum        VARCHAR(255),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
--  JUNCTION TABLES  (Many-to-Many)
-- ============================================

CREATE TABLE IF NOT EXISTS finding_evidence (
    finding_id      INTEGER NOT NULL REFERENCES findings(id) ON DELETE CASCADE,
    evidence_id     INTEGER NOT NULL REFERENCES evidence(id) ON DELETE CASCADE,
    PRIMARY KEY (finding_id, evidence_id)
);

CREATE TABLE IF NOT EXISTS claim_evidence (
    claim_id        INTEGER NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    evidence_id     INTEGER NOT NULL REFERENCES evidence(id) ON DELETE CASCADE,
    PRIMARY KEY (claim_id, evidence_id)
);

-- ============================================
--  INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_protocol_chains     ON protocol USING GIN(chains);
CREATE INDEX IF NOT EXISTS idx_contract_protocol   ON contract(protocol_id);
CREATE INDEX IF NOT EXISTS idx_source_protocol     ON source(protocol_id);
CREATE INDEX IF NOT EXISTS idx_document_source     ON document(source_id);
CREATE INDEX IF NOT EXISTS idx_findings_protocol   ON findings(protocol_id);
CREATE INDEX IF NOT EXISTS idx_claims_document     ON claims(document_id);
CREATE INDEX IF NOT EXISTS idx_finding_evidence_f  ON finding_evidence(finding_id);
CREATE INDEX IF NOT EXISTS idx_finding_evidence_e  ON finding_evidence(evidence_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_c    ON claim_evidence(claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_evidence_e    ON claim_evidence(evidence_id);
"""

# Tables and their primary-key column(s) for validation
VALID_TABLES: dict[str, list[str]] = {
    "protocol":          ["id"],
    "contract":          ["id"],
    "source":            ["id"],
    "document":          ["id"],
    "findings":          ["id"],
    "claims":            ["id"],
    "evidence":          ["id"],
    "finding_evidence":  ["finding_id", "evidence_id"],
    "claim_evidence":    ["claim_id", "evidence_id"],
}


# Database Manager

class DatabaseManager:
    """Full CRUD manager for the PSAT PostgreSQL database."""

    def __init__(
        self,
        dsn: str = None,
        dbname: str = "psat_db",
        user: str = "postgres",
        password: str = "postgres",
        host: str = "localhost",
        port: int = 5432,
    ):
        if dsn:
            self._dsn = dsn
            self._conn_params = None
        else:
            self._dsn = None
            self._conn_params = {
                "dbname": dbname,
                "user": user,
                "password": password,
                "host": host,
                "port": port,
            }
        self.conn: Optional[psycopg2.extensions.connection] = None

    # Connection helpers

    def connect(self) -> None:
        """Open a connection to the database."""
        if self.conn is None or self.conn.closed:
            if self._dsn:
                self.conn = psycopg2.connect(self._dsn)
            else:
                self.conn = psycopg2.connect(**self._conn_params)
            self.conn.autocommit = False

    def close(self) -> None:
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def _cursor(self):
        self.connect()
        return self.conn.cursor(cursor_factory=extras.RealDictCursor)

    # Initialisation

    def initialize(self) -> None:
        """Create all tables, indexes, and constraints (idempotent)."""
        self.connect()
        with self._cursor() as cur:
            cur.execute(SCHEMA_SQL)
        self.conn.commit()
        print("✔ Database schema initialised successfully.")

    # Validation

    @staticmethod
    def _validate_table(table: str) -> None:
        if table not in VALID_TABLES:
            raise ValueError(
                f"Unknown table '{table}'. Valid tables: {', '.join(VALID_TABLES)}"
            )

    # CREATE

    def insert(self, table: str, data: dict[str, Any]) -> dict:
        """
        Insert a row into *table* and return it (with generated id).

        Parameters
        ----------
        table : str
            Target table name.
        data : dict
            Column-name → value mapping. For the protocol table, pass
            ``chains`` as a Python list, e.g. ``["ethereum", "arbitrum"]``.

        Returns
        -------
        dict  – the inserted row.
        """
        self._validate_table(table)
        if not data:
            raise ValueError("data dict must not be empty.")

        columns = list(data.keys())
        values = list(data.values())

        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders}) RETURNING *").format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
        )

        with self._cursor() as cur:
            cur.execute(query, values)
            row = cur.fetchone()
        self.conn.commit()
        return dict(row)

    def bulk_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        """
        Insert multiple rows at once. Returns the number of rows inserted.
        All dicts must share the same keys.
        """
        self._validate_table(table)
        if not rows:
            return 0

        columns = list(rows[0].keys())
        values_list = [tuple(r[c] for c in columns) for r in rows]

        query = sql.SQL("INSERT INTO {table} ({fields}) VALUES %s").format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        )

        with self._cursor() as cur:
            extras.execute_values(cur, query.as_string(self.conn), values_list)
            count = cur.rowcount
        self.conn.commit()
        return count

    # READ

    def get_by_id(self, table: str, record_id: int) -> Optional[dict]:
        """Fetch a single row by its primary-key *id*."""
        self._validate_table(table)
        query = sql.SQL("SELECT * FROM {table} WHERE id = %s").format(
            table=sql.Identifier(table),
        )
        with self._cursor() as cur:
            cur.execute(query, (record_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    def get_all(
        self,
        table: str,
        where: Optional[dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> list[dict]:
        """
        Retrieve rows from *table* with optional filtering, ordering, and pagination.

        Parameters
        ----------
        where : dict, optional
            Column-name → value equality filters (ANDed together).
            To filter protocols by chain use ``get_protocols_by_chain`` instead,
            which uses the array-aware ``= ANY`` operator.
        order_by : str, optional
            Column name to sort by (prefix with ``-`` for DESC, e.g. ``"-created_at"``).
        limit / offset : int, optional
            Pagination controls.
        """
        self._validate_table(table)

        parts = [sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(table))]
        params: list[Any] = []

        # WHERE
        if where:
            clauses = []
            for col, val in where.items():
                clauses.append(
                    sql.SQL("{} = %s").format(sql.Identifier(col))
                )
                params.append(val)
            parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(clauses))

        # ORDER BY
        if order_by:
            if order_by.startswith("-"):
                parts.append(
                    sql.SQL("ORDER BY {} DESC").format(sql.Identifier(order_by[1:]))
                )
            else:
                parts.append(
                    sql.SQL("ORDER BY {} ASC").format(sql.Identifier(order_by))
                )

        # LIMIT / OFFSET
        if limit is not None:
            parts.append(sql.SQL("LIMIT %s"))
            params.append(limit)
        if offset is not None:
            parts.append(sql.SQL("OFFSET %s"))
            params.append(offset)

        query = sql.SQL(" ").join(parts)
        with self._cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    def search(self, table: str, column: str, pattern: str) -> list[dict]:
        """Case-insensitive ILIKE search on a text/varchar column."""
        self._validate_table(table)
        query = sql.SQL("SELECT * FROM {table} WHERE {col} ILIKE %s").format(
            table=sql.Identifier(table),
            col=sql.Identifier(column),
        )
        with self._cursor() as cur:
            cur.execute(query, (f"%{pattern}%",))
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    def count(self, table: str, where: Optional[dict[str, Any]] = None) -> int:
        """Return the row count, optionally filtered."""
        self._validate_table(table)
        parts = [sql.SQL("SELECT COUNT(*) AS cnt FROM {table}").format(table=sql.Identifier(table))]
        params: list[Any] = []
        if where:
            clauses = []
            for col, val in where.items():
                clauses.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
                params.append(val)
            parts.append(sql.SQL("WHERE ") + sql.SQL(" AND ").join(clauses))

        query = sql.SQL(" ").join(parts)
        with self._cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()["cnt"]

    # UPDATE

    def update(self, table: str, record_id: int, data: dict[str, Any]) -> Optional[dict]:
        """
        Update a row identified by *record_id*. Returns the updated row or None.
        For protocol rows, pass ``chains`` as a Python list to replace the full
        array. Use ``add_chain`` / ``remove_chain`` for surgical single-item updates.
        """
        self._validate_table(table)
        if not data:
            raise ValueError("data dict must not be empty.")

        set_clauses = []
        values: list[Any] = []
        for col, val in data.items():
            set_clauses.append(
                sql.SQL("{} = %s").format(sql.Identifier(col))
            )
            values.append(val)
        values.append(record_id)

        query = sql.SQL("UPDATE {table} SET {sets} WHERE id = %s RETURNING *").format(
            table=sql.Identifier(table),
            sets=sql.SQL(", ").join(set_clauses),
        )

        with self._cursor() as cur:
            cur.execute(query, values)
            row = cur.fetchone()
        self.conn.commit()
        return dict(row) if row else None

    # DELETE

    def delete(self, table: str, record_id: int) -> bool:
        """Delete a row by primary-key *id*. Returns True if a row was deleted."""
        self._validate_table(table)
        query = sql.SQL("DELETE FROM {table} WHERE id = %s").format(
            table=sql.Identifier(table),
        )
        with self._cursor() as cur:
            cur.execute(query, (record_id,))
            deleted = cur.rowcount > 0
        self.conn.commit()
        return deleted

    # Protocol chain helpers

    def add_chain(self, protocol_id: int, chain: str) -> Optional[dict]:
        """
        Append *chain* to a protocol's chains array (no-op if already present).
        Returns the updated protocol row, or None if the protocol doesn't exist.
        """
        query = """
            UPDATE protocol
            SET chains = ARRAY(
                SELECT DISTINCT unnest(chains || ARRAY[%s::VARCHAR]) ORDER BY 1
            )
            WHERE id = %s
            RETURNING *
        """
        with self._cursor() as cur:
            cur.execute(query, (chain, protocol_id))
            row = cur.fetchone()
        self.conn.commit()
        return dict(row) if row else None

    def remove_chain(self, protocol_id: int, chain: str) -> Optional[dict]:
        """
        Remove *chain* from a protocol's chains array.
        Returns the updated protocol row, or None if the protocol doesn't exist.
        """
        query = """
            UPDATE protocol
            SET chains = ARRAY(
                SELECT c FROM unnest(chains) AS c WHERE c <> %s
            )
            WHERE id = %s
            RETURNING *
        """
        with self._cursor() as cur:
            cur.execute(query, (chain, protocol_id))
            row = cur.fetchone()
        self.conn.commit()
        return dict(row) if row else None

    def get_protocols_by_chain(self, chain: str) -> list[dict]:
        """
        Return all protocols associated with *chain*.
        Uses the GIN-indexed ``= ANY`` operator for efficient lookup.
        """
        query = "SELECT * FROM protocol WHERE %s = ANY(chains) ORDER BY id"
        with self._cursor() as cur:
            cur.execute(query, (chain,))
            return [dict(r) for r in cur.fetchall()]

    def get_protocols_by_chains(self, chains: list[str], match_all: bool = False) -> list[dict]:
        """
        Return protocols matching the given *chains*.

        Parameters
        ----------
        chains : list[str]
            Chain names to filter by.
        match_all : bool
            If True, return only protocols present on *all* supplied chains
            (array containment: ``chains @> %s``).
            If False (default), return protocols on *any* of the supplied chains
            (array overlap: ``chains && %s``).
        """
        operator = "@>" if match_all else "&&"
        query = f"SELECT * FROM protocol WHERE chains {operator} %s::VARCHAR[] ORDER BY id"
        with self._cursor() as cur:
            cur.execute(query, (chains,))
            return [dict(r) for r in cur.fetchall()]

    # Junction-table helpers

    def link_finding_evidence(self, finding_id: int, evidence_id: int) -> dict:
        """Create a finding ↔ evidence association."""
        return self.insert("finding_evidence", {
            "finding_id": finding_id,
            "evidence_id": evidence_id,
        })

    def unlink_finding_evidence(self, finding_id: int, evidence_id: int) -> bool:
        """Remove a finding ↔ evidence association."""
        query = sql.SQL(
            "DELETE FROM finding_evidence WHERE finding_id = %s AND evidence_id = %s"
        )
        with self._cursor() as cur:
            cur.execute(query, (finding_id, evidence_id))
            removed = cur.rowcount > 0
        self.conn.commit()
        return removed

    def link_claim_evidence(self, claim_id: int, evidence_id: int) -> dict:
        """Create a claim ↔ evidence association."""
        return self.insert("claim_evidence", {
            "claim_id": claim_id,
            "evidence_id": evidence_id,
        })

    def unlink_claim_evidence(self, claim_id: int, evidence_id: int) -> bool:
        """Remove a claim ↔ evidence association."""
        query = sql.SQL(
            "DELETE FROM claim_evidence WHERE claim_id = %s AND evidence_id = %s"
        )
        with self._cursor() as cur:
            cur.execute(query, (claim_id, evidence_id))
            removed = cur.rowcount > 0
        self.conn.commit()
        return removed

    def get_evidence_for_finding(self, finding_id: int) -> list[dict]:
        """Return all evidence rows linked to a finding."""
        query = """
            SELECT e.* FROM evidence e
            JOIN finding_evidence fe ON e.id = fe.evidence_id
            WHERE fe.finding_id = %s
        """
        with self._cursor() as cur:
            cur.execute(query, (finding_id,))
            return [dict(r) for r in cur.fetchall()]

    def get_evidence_for_claim(self, claim_id: int) -> list[dict]:
        """Return all evidence rows linked to a claim."""
        query = """
            SELECT e.* FROM evidence e
            JOIN claim_evidence ce ON e.id = ce.evidence_id
            WHERE ce.claim_id = %s
        """
        with self._cursor() as cur:
            cur.execute(query, (claim_id,))
            return [dict(r) for r in cur.fetchall()]

    # Raw SQL escape hatch

    def execute_raw(self, query: str, params: tuple = ()) -> list[dict]:
        """Run an arbitrary SELECT and return results as dicts."""
        with self._cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return [dict(r) for r in cur.fetchall()]
        self.conn.commit()
        return []

    # Context manager support

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        self.close()
        return False

    def __repr__(self):
        status = "connected" if (self.conn and not self.conn.closed) else "disconnected"
        label = self._dsn[:40] + "..." if self._dsn else self._conn_params['dbname']
        return f"<DatabaseManager db={label!r} {status}>"


# Interactive CLI

def _print_rows(rows: list[dict]) -> None:
    if not rows:
        print("  (no results)")
        return
    for i, row in enumerate(rows, 1):
        print(f"  [{i}] {row}")


def _prompt_dict(prompt_msg: str = "Enter column=value pairs (comma-separated): ") -> dict:
    raw = input(prompt_msg).strip()
    if not raw:
        return {}
    pairs = {}
    for token in raw.split(","):
        if "=" not in token:
            print(f"  ⚠ Skipping invalid token: {token!r}")
            continue
        key, val = token.split("=", 1)
        key = key.strip()
        val = val.strip()
        # Simple type coercion
        if val.lower() in ("true", "false"):
            val = val.lower() == "true"
        else:
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    pass  # keep as string
        pairs[key] = val
    return pairs


def interactive_cli():
    """Launch a simple REPL for managing the PSAT database."""
    print("=" * 60)
    print("  PSAT Database Manager — Interactive CLI")
    print("=" * 60)

    dsn = input("Connection string (or press Enter for manual config): ").strip()

    if dsn:
        db = DatabaseManager(dsn=dsn)
    else:
        dbname   = input("Database name [psat_db]: ").strip() or "psat_db"
        user     = input("User [postgres]: ").strip() or "postgres"
        password = input("Password [postgres]: ").strip() or "postgres"
        host     = input("Host [localhost]: ").strip() or "localhost"
        port     = input("Port [5432]: ").strip() or "5432"
        db = DatabaseManager(dbname=dbname, user=user, password=password, host=host, port=int(port))

    try:
        db.initialize()
    except psycopg2.OperationalError as e:
        print(f"\n✘ Could not connect to database: {e}")
        print("  Make sure PostgreSQL is running and the connection details are correct.")
        return

    tables = list(VALID_TABLES.keys())

    HELP = """
  Commands:
    list                        — Show all tables
    get    <table>              — Show all rows
    find   <table> <id>         — Show row by ID
    search <table> <col> <pat>  — ILIKE search
    add    <table>              — Insert a new row
                                  (for protocol, use chains=ethereum|arbitrum)
    edit   <table> <id>         — Update a row
                                  (for protocol, chains=ethereum|arbitrum replaces all)
    rm     <table> <id>         — Delete a row

    chain-add <protocol_id> <chain>   — Add one chain to a protocol
    chain-rm  <protocol_id> <chain>   — Remove one chain from a protocol
    chain-ls  <chain>                 — List protocols on a chain

    link   <junction>           — Link finding/claim ↔ evidence
    unlink <junction>           — Unlink finding/claim ↔ evidence
    sql    <query>              — Run raw SELECT
    count  <table>              — Row count
    help                        — Show this message
    quit                        — Exit
"""
    print(HELP)

    while True:
        try:
            raw = input("\npsat> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0].lower()

        try:
            if cmd in ("quit", "exit", "q"):
                break

            elif cmd == "help":
                print(HELP)

            elif cmd == "list":
                print("  Tables:", ", ".join(tables))

            elif cmd == "get":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                where = None
                filt = input("  Filter (col=val,… or blank): ").strip()
                if filt:
                    where = {}
                    for t in filt.split(","):
                        if "=" in t:
                            k, v = t.split("=", 1)
                            where[k.strip()] = v.strip()
                rows = db.get_all(table, where=where or None, order_by="-created_at")
                _print_rows(rows)

            elif cmd == "find":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                rid = int(parts[2]) if len(parts) > 2 else int(input("  ID: ").strip())
                row = db.get_by_id(table, rid)
                if row:
                    for k, v in row.items():
                        print(f"  {k}: {v}")
                else:
                    print(f"  Not found (id={rid})")

            elif cmd == "search":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                col = parts[2] if len(parts) > 2 else input("  Column: ").strip()
                pattern = " ".join(parts[3:]) if len(parts) > 3 else input("  Pattern: ").strip()
                rows = db.search(table, col, pattern)
                _print_rows(rows)

            elif cmd == "add":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                data = _prompt_dict("  Column=value pairs (comma-separated): ")
                # chains are pipe-delimited to avoid clashing with the comma separator
                # e.g. chains=ethereum|arbitrum|optimism
                if "chains" in data and isinstance(data["chains"], str):
                    data["chains"] = [c.strip() for c in data["chains"].split("|") if c.strip()]
                if data:
                    row = db.insert(table, data)
                    print(f"  ✔ Inserted: {row}")
                else:
                    print("  ⚠ No data provided.")

            elif cmd == "edit":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                rid = int(parts[2]) if len(parts) > 2 else int(input("  ID: ").strip())
                data = _prompt_dict("  Column=value pairs to update (comma-separated): ")
                if "chains" in data and isinstance(data["chains"], str):
                    data["chains"] = [c.strip() for c in data["chains"].split("|") if c.strip()]
                if data:
                    row = db.update(table, rid, data)
                    if row:
                        print(f"  ✔ Updated: {row}")
                    else:
                        print(f"  Not found (id={rid})")
                else:
                    print("  ⚠ No data provided.")

            elif cmd == "rm":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                rid = int(parts[2]) if len(parts) > 2 else int(input("  ID: ").strip())
                if db.delete(table, rid):
                    print(f"  ✔ Deleted id={rid} from {table}.")
                else:
                    print(f"  Not found (id={rid})")

            elif cmd == "chain-add":
                pid = int(parts[1]) if len(parts) > 1 else int(input("  Protocol ID: ").strip())
                chain = parts[2] if len(parts) > 2 else input("  Chain: ").strip()
                row = db.add_chain(pid, chain)
                if row:
                    print(f"  ✔ Chains now: {row['chains']}")
                else:
                    print(f"  Not found (id={pid})")

            elif cmd == "chain-rm":
                pid = int(parts[1]) if len(parts) > 1 else int(input("  Protocol ID: ").strip())
                chain = parts[2] if len(parts) > 2 else input("  Chain: ").strip()
                row = db.remove_chain(pid, chain)
                if row:
                    print(f"  ✔ Chains now: {row['chains']}")
                else:
                    print(f"  Not found (id={pid})")

            elif cmd == "chain-ls":
                chain = parts[1] if len(parts) > 1 else input("  Chain: ").strip()
                rows = db.get_protocols_by_chain(chain)
                _print_rows(rows)

            elif cmd == "link":
                jt = parts[1] if len(parts) > 1 else input("  Junction (finding_evidence / claim_evidence): ").strip()
                if jt == "finding_evidence":
                    fid = int(input("  Finding ID: ").strip())
                    eid = int(input("  Evidence ID: ").strip())
                    row = db.link_finding_evidence(fid, eid)
                    print(f"  ✔ Linked: {row}")
                elif jt == "claim_evidence":
                    cid = int(input("  Claim ID: ").strip())
                    eid = int(input("  Evidence ID: ").strip())
                    row = db.link_claim_evidence(cid, eid)
                    print(f"  ✔ Linked: {row}")
                else:
                    print(f"  Unknown junction: {jt}")

            elif cmd == "unlink":
                jt = parts[1] if len(parts) > 1 else input("  Junction (finding_evidence / claim_evidence): ").strip()
                if jt == "finding_evidence":
                    fid = int(input("  Finding ID: ").strip())
                    eid = int(input("  Evidence ID: ").strip())
                    ok = db.unlink_finding_evidence(fid, eid)
                    print("  ✔ Unlinked." if ok else "  Link not found.")
                elif jt == "claim_evidence":
                    cid = int(input("  Claim ID: ").strip())
                    eid = int(input("  Evidence ID: ").strip())
                    ok = db.unlink_claim_evidence(cid, eid)
                    print("  ✔ Unlinked." if ok else "  Link not found.")
                else:
                    print(f"  Unknown junction: {jt}")

            elif cmd == "sql":
                query = " ".join(parts[1:]) if len(parts) > 1 else input("  SQL> ").strip()
                rows = db.execute_raw(query)
                _print_rows(rows)

            elif cmd == "count":
                table = parts[1] if len(parts) > 1 else input("  Table: ").strip()
                print(f"  {db.count(table)} row(s)")

            else:
                print(f"  Unknown command: {cmd}. Type 'help' for options.")

        except Exception as e:
            print(f"  ✘ Error: {e}")
            if db.conn and not db.conn.closed:
                db.conn.rollback()

    db.close()
    print("\nGoodbye!")

#  Entry Point

if __name__ == "__main__":
    interactive_cli()
