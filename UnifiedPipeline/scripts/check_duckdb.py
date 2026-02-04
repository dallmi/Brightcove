#!/usr/bin/env python3
"""
Quick diagnostic to check DuckDB analytics data coverage.
"""

import duckdb
from pathlib import Path

# Adjust this path to your DuckDB file
DB_PATH = Path("P:/IMPORTANT/Projects/brightcove_ori/UnifiedPipeline/output/analytics.duckdb")

def main():
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return

    conn = duckdb.connect(str(DB_PATH), read_only=True)

    print("=" * 70)
    print("DuckDB Analytics Data Coverage")
    print("=" * 70)

    # 1. Overall stats
    print("\n1. OVERALL STATS:")
    result = conn.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT video_id) as unique_videos,
            COUNT(DISTINCT account_name) as accounts,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM daily_analytics
    """).fetchone()
    print(f"   Total rows: {result[0]:,}")
    print(f"   Unique videos: {result[1]:,}")
    print(f"   Accounts: {result[2]}")
    print(f"   Date range: {result[3]} to {result[4]}")

    # 2. Per-account breakdown
    print("\n2. PER-ACCOUNT BREAKDOWN:")
    print(f"   {'Account':<25} {'Rows':>12} {'Videos':>10} {'Min Date':>12} {'Max Date':>12}")
    print("   " + "-" * 75)

    results = conn.execute("""
        SELECT
            account_name,
            COUNT(*) as rows,
            COUNT(DISTINCT video_id) as videos,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM daily_analytics
        GROUP BY account_name
        ORDER BY account_name
    """).fetchall()

    for row in results:
        print(f"   {row[0]:<25} {row[1]:>12,} {row[2]:>10,} {row[3]!s:>12} {row[4]!s:>12}")

    # 3. Per-year breakdown
    print("\n3. PER-YEAR BREAKDOWN:")
    print(f"   {'Year':<6} {'Rows':>15} {'Accounts':>10}")
    print("   " + "-" * 35)

    results = conn.execute("""
        SELECT
            EXTRACT(YEAR FROM date) as year,
            COUNT(*) as rows,
            COUNT(DISTINCT account_name) as accounts
        FROM daily_analytics
        GROUP BY year
        ORDER BY year
    """).fetchall()

    for row in results:
        print(f"   {int(row[0]):<6} {row[1]:>15,} {row[2]:>10}")

    # 4. Per-account per-year (to find gaps)
    print("\n4. PER-ACCOUNT PER-YEAR (checking for gaps):")
    print(f"   {'Account':<25} {'2024':>12} {'2025':>12} {'2026':>12}")
    print("   " + "-" * 65)

    results = conn.execute("""
        SELECT
            account_name,
            SUM(CASE WHEN EXTRACT(YEAR FROM date) = 2024 THEN 1 ELSE 0 END) as y2024,
            SUM(CASE WHEN EXTRACT(YEAR FROM date) = 2025 THEN 1 ELSE 0 END) as y2025,
            SUM(CASE WHEN EXTRACT(YEAR FROM date) = 2026 THEN 1 ELSE 0 END) as y2026
        FROM daily_analytics
        GROUP BY account_name
        ORDER BY account_name
    """).fetchall()

    for row in results:
        y2024 = f"{row[1]:,}" if row[1] > 0 else "MISSING!"
        y2025 = f"{row[2]:,}" if row[2] > 0 else "MISSING!"
        y2026 = f"{row[3]:,}" if row[3] > 0 else "MISSING!"
        print(f"   {row[0]:<25} {y2024:>12} {y2025:>12} {y2026:>12}")

    # 5. Check specifically for Intranet
    print("\n5. INTRANET DETAILED CHECK:")
    result = conn.execute("""
        SELECT
            COUNT(*) as rows,
            COUNT(DISTINCT video_id) as videos,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM daily_analytics
        WHERE LOWER(account_name) = 'intranet'
    """).fetchone()

    if result[0] > 0:
        print(f"   Rows: {result[0]:,}")
        print(f"   Videos: {result[1]:,}")
        print(f"   Date range: {result[2]} to {result[3]}")
    else:
        print("   NO DATA FOUND FOR INTRANET!")

        # Check what account names exist
        print("\n   Available account names:")
        results = conn.execute("SELECT DISTINCT account_name FROM daily_analytics ORDER BY account_name").fetchall()
        for row in results:
            print(f"     - {row[0]}")

    conn.close()
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
