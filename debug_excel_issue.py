"""
debug_excel_issue.py - Diagnose why Excel output has blank columns

Compares data at each stage of the pipeline for working vs broken accounts.
"""

import json
from pathlib import Path

# Try to import pandas
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("WARNING: pandas not installed, skipping DataFrame tests")

# =============================================================================
# CONFIGURATION - Edit these
# =============================================================================

# Accounts to compare (working vs broken)
WORKING_ACCOUNT = "circleone"
BROKEN_ACCOUNTS = ["Internet", "Intranet"]

# Base paths
BASE_PATH = Path("UnifiedPipeline/output")
CMS_PATH = BASE_PATH / "cms"
ANALYTICS_PATH = BASE_PATH / "analytics"

# Fields we expect to see populated
EXPECTED_FIELDS = [
    "account_id", "id", "name", "original_filename", "description",
    "created_at", "published_at", "updated_at", "duration", "state",
    "created_by", "reference_id", "tags", "dt_last_viewed"
]


# =============================================================================
# DIAGNOSTIC FUNCTIONS
# =============================================================================

def check_json_file(filepath, label):
    """Check if JSON file exists and what fields the first record has."""
    print(f"\n{'='*60}")
    print(f"Checking: {label}")
    print(f"File: {filepath}")
    print("="*60)

    if not filepath.exists():
        print("  ERROR: File not found!")
        return None

    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"  Total records: {len(data)}")

    if not data:
        print("  ERROR: File is empty!")
        return None

    first = data[0]
    print(f"  Fields in first record: {len(first.keys())}")

    # Check expected fields
    print(f"\n  Expected fields check:")
    missing = []
    empty = []
    present = []

    for field in EXPECTED_FIELDS:
        if field not in first:
            missing.append(field)
        elif first[field] is None or first[field] == "":
            empty.append(field)
        else:
            present.append(field)

    print(f"    Present with data: {len(present)}")
    if present:
        for f in present[:5]:  # Show first 5
            val = str(first[f])[:50]  # Truncate long values
            print(f"      - {f}: {val}")
        if len(present) > 5:
            print(f"      ... and {len(present) - 5} more")

    if empty:
        print(f"    Present but empty: {empty}")

    if missing:
        print(f"    MISSING: {missing}")

    return data


def check_pandas_dataframe(data, label):
    """Check how pandas interprets the data."""
    if not HAS_PANDAS:
        return

    print(f"\n  Pandas DataFrame check:")

    df = pd.DataFrame(data)
    print(f"    Shape: {df.shape}")
    print(f"    Columns: {len(df.columns)}")

    # Check expected fields in DataFrame
    missing_in_df = [f for f in EXPECTED_FIELDS if f not in df.columns]
    if missing_in_df:
        print(f"    MISSING from DataFrame: {missing_in_df}")

    # Check for columns with all null/empty values
    empty_cols = []
    for col in EXPECTED_FIELDS:
        if col in df.columns:
            non_null = df[col].notna().sum()
            non_empty = (df[col] != "").sum() if df[col].dtype == object else non_null
            if non_null == 0 or non_empty == 0:
                empty_cols.append(col)

    if empty_cols:
        print(f"    Columns with ALL empty values: {empty_cols}")
    else:
        print(f"    All expected columns have data")

    # Sample values for key fields
    print(f"\n    Sample values (first record):")
    for field in ["created_at", "duration", "published_at", "dt_last_viewed"]:
        if field in df.columns:
            val = df[field].iloc[0]
            print(f"      {field}: {val}")


def compare_accounts():
    """Compare working and broken accounts."""

    print("\n" + "#"*60)
    print("# WORKING ACCOUNT")
    print("#"*60)

    # Check working account - CMS metadata
    cms_path = CMS_PATH / f"{WORKING_ACCOUNT}_cms_metadata.json"
    working_cms = check_json_file(cms_path, f"{WORKING_ACCOUNT} - CMS Metadata (Script 1)")

    # Check working account - Enriched
    enriched_path = ANALYTICS_PATH / f"{WORKING_ACCOUNT}_cms_enriched.json"
    working_enriched = check_json_file(enriched_path, f"{WORKING_ACCOUNT} - Enriched (Script 2)")

    if working_enriched and HAS_PANDAS:
        check_pandas_dataframe(working_enriched, WORKING_ACCOUNT)

    # Check broken accounts
    for account in BROKEN_ACCOUNTS:
        print("\n" + "#"*60)
        print(f"# BROKEN ACCOUNT: {account}")
        print("#"*60)

        # CMS metadata
        cms_path = CMS_PATH / f"{account}_cms_metadata.json"
        broken_cms = check_json_file(cms_path, f"{account} - CMS Metadata (Script 1)")

        # Enriched
        enriched_path = ANALYTICS_PATH / f"{account}_cms_enriched.json"
        broken_enriched = check_json_file(enriched_path, f"{account} - Enriched (Script 2)")

        if broken_enriched and HAS_PANDAS:
            check_pandas_dataframe(broken_enriched, account)

        # Compare field sets if both exist
        if working_cms and broken_cms:
            print(f"\n  Field comparison vs {WORKING_ACCOUNT}:")
            working_keys = set(working_cms[0].keys())
            broken_keys = set(broken_cms[0].keys())

            only_in_working = working_keys - broken_keys
            only_in_broken = broken_keys - working_keys

            if only_in_working:
                print(f"    Fields only in {WORKING_ACCOUNT}: {only_in_working}")
            if only_in_broken:
                print(f"    Fields only in {account}: {only_in_broken}")
            if not only_in_working and not only_in_broken:
                print(f"    Both have identical field sets")


def main():
    print("="*60)
    print("Excel Blank Columns Diagnostic")
    print("="*60)
    print(f"\nWorking account: {WORKING_ACCOUNT}")
    print(f"Broken accounts: {BROKEN_ACCOUNTS}")
    print(f"Base path: {BASE_PATH}")

    compare_accounts()

    print("\n" + "="*60)
    print("DIAGNOSIS COMPLETE")
    print("="*60)
    print("""
Next steps based on results:
- If CMS JSON is missing fields → Issue in Script 1 or Brightcove API
- If Enriched JSON is missing fields → Issue in Script 2 enrich_cms_metadata()
- If DataFrame is missing fields → Issue in pandas conversion
- If all have fields but Excel doesn't → Issue in write_lifecycle_excel()
""")


if __name__ == "__main__":
    main()
