#!/usr/bin/env python3
"""Import database tables from a JSON export file."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from db.models import Base, SessionLocal, create_tables, engine


def import_db(input_path: str = "db_export.json", clear: bool = True) -> None:
    data = json.loads(Path(input_path).read_text())
    tables = data.get("tables", {})

    print(f"Importing from {input_path} (exported {data.get('exported_at', '?')})")

    # Ensure tables exist
    create_tables()

    with SessionLocal() as session:
        if clear:
            print("Clearing existing data...")
            # Disable FK checks temporarily for clean import
            session.execute(text("SET session_replication_role = 'replica'"))
            for table_name in reversed(list(Base.metadata.sorted_tables)):
                if table_name.name in tables:
                    session.execute(text(f"DELETE FROM {table_name.name}"))
            session.execute(text("SET session_replication_role = 'origin'"))
            session.commit()

        # Import in dependency order
        for table in Base.metadata.sorted_tables:
            table_name = table.name
            rows = tables.get(table_name, [])
            if not rows:
                continue

            columns = [c.name for c in table.columns]
            for row in rows:
                # Filter to only columns that exist in the table
                filtered = {k: v for k, v in row.items() if k in columns}
                if filtered:
                    session.execute(table.insert().values(**filtered))

            session.commit()
            print(f"  {table_name}: {len(rows)} rows imported")

        # Reset sequences for serial columns
        for table in Base.metadata.sorted_tables:
            for col in table.columns:
                if col.autoincrement and hasattr(col.type, "impl"):
                    seq_name = f"{table.name}_{col.name}_seq"
                    try:
                        session.execute(text(
                            f"SELECT setval('{seq_name}', COALESCE((SELECT MAX({col.name}) FROM {table.name}), 0) + 1, false)"
                        ))
                    except Exception:
                        pass
            session.commit()

    print("\nImport complete.")


if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "db_export.json"
    import_db(input_file)
