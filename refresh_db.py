"""Script to refresh the database from all local XML files (MOHA, UN, PEP)"""
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

import config
from database import get_database
from xml_parser import parse_local_file


def refresh_source(db, source_key: str, source_label: str, parser_type: str):
    """Refresh a single source in the database."""
    print(f"\n{'=' * 60}")
    print(f"Refreshing {source_label} in database")
    print('=' * 60)

    xml_file = config.LOCAL_XML_FILES.get(source_key)
    if not xml_file or not xml_file.exists():
        print(f"ERROR: {source_label} file not found at {xml_file}")
        return

    print(f"Loading {source_label} from: {xml_file}")

    records = parse_local_file(str(xml_file), source_type=parser_type)

    if not records:
        print(f"ERROR: No records parsed from {source_label}")
        return

    print(f"Parsed {len(records)} records from {source_label}")

    # Clear existing records for this source to avoid duplicates
    print(f"Clearing existing {source_label} records from database...")
    conn = db._get_connection()
    cursor = conn.cursor()
    # Map source_key to the source value stored in DB
    db_source_map = {
        "MOHA_MALAYSIA": "MOHA_MALAYSIA",
        "UN": "UN",
        "PEP_LIST": "PEP_LIST",
    }
    db_source = db_source_map.get(source_label, source_label)
    cursor.execute("DELETE FROM sanctions WHERE source = ?", (db_source,))
    conn.commit()
    conn.close()
    print(f"Cleared existing {source_label} records")

    # Add new records
    print(f"Adding parsed records to database...")
    result = db.add_sanctions_batch(records, source=source_label)

    print(f"\nResults:")
    print(f"  Added: {result['added']}")
    print(f"  Skipped: {result['skipped']}")


def main():
    db = get_database()

    # Refresh MOHA Malaysia list
    refresh_source(db, "MOHA_MALAYSIA", "MOHA_MALAYSIA", "MOHA")

    # Refresh UN sanctions list
    refresh_source(db, "UN_LIST", "UN_LIST", "UN")

    # Refresh PEP list
    refresh_source(db, "PEP_LIST", "PEP_LIST", "PEP")

    # Print final summary
    print(f"\n{'=' * 60}")
    print("Database refresh complete!")
    print('=' * 60)

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT source, COUNT(*) as cnt FROM sanctions WHERE is_active = 1 GROUP BY source")
    rows = cursor.fetchall()
    cursor.execute("SELECT COUNT(*) FROM sanctions WHERE is_active = 1")
    total = cursor.fetchone()[0]
    conn.close()

    for row in rows:
        print(f"  {row['source']}: {row['cnt']} records")
    print(f"  Total: {total} records")


if __name__ == "__main__":
    main()