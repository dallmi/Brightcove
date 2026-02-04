#!/usr/bin/env python3
"""
Quick diagnostic to check dt_last_viewed in lifecycle Excel files.
"""

import pandas as pd
from pathlib import Path

# Paths
UNIFIED_DIR = Path("P:/IMPORTANT/Projects/brightcove_ori/UnifiedPipeline/output/life_cycle_mgmt/2026-02")
HARPER_DIR = Path("P:/IMPORTANT/Projects/brightcove_ori/Harper/csv")

def check_excel(path, label):
    """Check dt_last_viewed in an Excel/CSV file."""
    print(f"\n{'='*60}")
    print(f"{label}: {path.name}")
    print('='*60)

    if not path.exists():
        print(f"  FILE NOT FOUND: {path}")
        return

    try:
        if path.suffix == '.xlsx':
            df = pd.read_excel(path, dtype=str)
        else:
            df = pd.read_csv(path, dtype=str)

        print(f"  Total rows: {len(df)}")
        print(f"  Columns: {len(df.columns)}")

        if 'dt_last_viewed' in df.columns:
            non_null = df['dt_last_viewed'].notna() & (df['dt_last_viewed'] != '') & (df['dt_last_viewed'] != 'None')
            print(f"  dt_last_viewed column: EXISTS")
            print(f"  dt_last_viewed non-empty: {non_null.sum()} of {len(df)} ({100*non_null.sum()/len(df):.1f}%)")

            # Sample values
            print(f"\n  Sample values (first 5 with dt_last_viewed):")
            sample = df[non_null][['id', 'name', 'dt_last_viewed']].head(5)
            if len(sample) > 0:
                for _, row in sample.iterrows():
                    print(f"    ID: {row['id']}, dt_last_viewed: {row['dt_last_viewed']}")
            else:
                print(f"    (no rows with dt_last_viewed)")

            # Sample empty values
            print(f"\n  Sample values (first 5 WITHOUT dt_last_viewed):")
            empty = df[~non_null][['id', 'name', 'dt_last_viewed']].head(5)
            for _, row in empty.iterrows():
                print(f"    ID: {row['id']}, dt_last_viewed: '{row['dt_last_viewed']}'")
        else:
            print(f"  dt_last_viewed column: MISSING!")
            print(f"  Available columns: {list(df.columns)[:10]}...")

    except Exception as e:
        print(f"  ERROR: {e}")


def main():
    print("Checking dt_last_viewed in lifecycle files")
    print("="*60)

    # Check UnifiedPipeline Excel files
    accounts = ['Internet', 'Intranet', 'neo', 'circleone']  # Mix of potentially problematic and working

    for account in accounts:
        # UnifiedPipeline Excel
        excel_path = UNIFIED_DIR / f"{account}_cms.xlsx"
        check_excel(excel_path, f"UnifiedPipeline - {account}")

        # Harper CSV for comparison
        csv_path = HARPER_DIR / f"{account.lower()}_cms.csv"
        if not csv_path.exists():
            csv_path = HARPER_DIR / f"{account}_cms.csv"
        check_excel(csv_path, f"Harper - {account}")

    print("\n" + "="*60)
    print("DONE")
    print("="*60)


if __name__ == "__main__":
    main()
