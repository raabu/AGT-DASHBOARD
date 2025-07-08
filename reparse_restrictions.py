import sqlite3
import pandas as pd
from db_utils import initialize_restriction_table, store_restrictions
from scrape_notices import extract_restriction_block, parse_restriction_table

DB_PATH = "notices.db"

def reparse_all_restrictions():
    # Connect to DB and load full notices
    conn = sqlite3.connect(DB_PATH)
    query = """
    SELECT notice_number, full_notice
    FROM notices
    WHERE LOWER(TRIM(type)) = 'capacity constraint'
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"🔄 Found {len(df)} capacity constraint notices to re-parse")

    # Reinitialize the restrictions table (clears it)
    initialize_restriction_table()

    success_count = 0
    fail_count = 0

    for row in df.itertuples(index=False):
        pipe = "AGT"

        if pipe != "AGT":
            print(f"⚠️ Skipping unsupported pipeline: {pipe}")
            continue

        try:
            restriction_lines = extract_restriction_block(row.full_notice)
            restriction_data = parse_restriction_table(restriction_lines)


            if restriction_data:
                store_restrictions(row.notice_number, restriction_data)
                print(f"✅ Parsed & stored restrictions for: {row.notice_number}")
                success_count += 1
            else:
                print(f"⚠️ No restrictions found for: {row.notice_number}")
        except Exception as e:
            print(f"❌ Failed to parse {row.notice_number}: {e}")
            fail_count += 1

    print("\n🎉 Re-parse complete!")
    print(f"✅ Successful: {success_count}")
    print(f"❌ Failed: {fail_count}")

def reshape_restriction_rows(raw_lines: list[list[str]]) -> list[dict]:
    """
    Takes raw lines (list of lists) and reshapes them into structured restriction rows
    with columns: location, scheduled, AO, IT, 3B, 3A, 2C, 2B, 2A, 1
    """
    structured_rows = []

    # Define priority labels in expected order
    priority_columns = ["AO", "IT", "3B", "3A", "2C", "2B", "2A", "1"]

    # Define keywords that are NOT data rows
    skip_keywords = {
        "Restricted Locations", "Scheduled and Sealed", "Priority % Restricted", "Notes",
        "AO", "IT", "3B", "3A", "2C", "2B", "2A", "1"
    }

    for line in raw_lines:
        if not line:
            continue  # Skip empty

        location = line[0].strip()
        if location in skip_keywords:
            continue

        entry = {
            "location": location,
            "scheduled": None,
        }

        values = line[1:]  # everything after the location
        priority_values = []

        # Heuristic: single "Yes" = scheduled only
        if len(values) == 1 and values[0].strip().lower() == "yes":
            entry["scheduled"] = "Yes"

        else:
            # Look for "Yes" in any position for scheduled
            if "Yes" in values:
                entry["scheduled"] = "Yes"

            # Otherwise look for scheduled as leading "100%" trio
            elif len(values) >= 3 and all("%" in val for val in values[:3]):
                entry["scheduled"] = values[0]  # assume first 100% is scheduled
                priority_values = values[1:]     # rest are AO → 1

            # If we can’t figure it out, assign everything as priorities
            else:
                priority_values = values

        # Map priority values to columns (pad or truncate)
        for i, col in enumerate(priority_columns):
            entry[col] = priority_values[i] if i < len(priority_values) else ""

        structured_rows.append(entry)

    return structured_rows

if __name__ == "__main__":
    reparse_all_restrictions()
