#!/usr/bin/env python3
"""Export all database tables to a JSON file."""

import json
import sys
from datetime import datetime
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import inspect, select

from db.models import Base, SessionLocal, engine


def _serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def export_db(output_path: str = "db_export.json") -> None:
    inspector = inspect(engine)
    table_names = sorted(inspector.get_table_names())
    # Skip alembic
    table_names = [t for t in table_names if t != "alembic_version"]

    data = {"exported_at": datetime.utcnow().isoformat(), "tables": {}}

    with SessionLocal() as session:
        for table_name in table_names:
            table = Base.metadata.tables.get(table_name)
            if table is None:
                continue
            rows = session.execute(select(table)).fetchall()
            columns = [c.name for c in table.columns]
            serialized = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col] = _serialize(row[i])
                serialized.append(row_dict)
            data["tables"][table_name] = serialized
            print(f"  {table_name}: {len(serialized)} rows")

    Path(output_path).write_text(json.dumps(data, indent=2, default=str))
    print(f"\nExported to {output_path}")


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "db_export.json"
    export_db(output)
