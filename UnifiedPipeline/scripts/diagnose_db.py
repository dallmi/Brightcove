"""
diagnose_db.py - Diagnostic tool for DuckDB analytics database

Run this to check database state and debug incremental processing issues.
"""

import sys
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Diagnose DuckDB analytics database")
    parser.add_argument('--db', type=str, help='Path to DuckDB file (default: output/analytics.duckdb)')
    parser.add_argument('--account', type=str, help='Focus on specific account name')
    args = parser.parse_args()

    # Determine DB path
    script_dir = Path(__file__).parent
    output_dir = script_dir.parent / 'output'

    if args.db:
        db_path = Path(args.db)
    else:
        db_path = output_dir / 'analytics.duckdb'

    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        print(f"Available .duckdb files in output/:")
        for f in output_dir.glob("*.duckdb"):
            print(f"  {f.name}")
        return 1

    import duckdb
    conn = duckdb.connect(str(db_path), read_only=True)

    print("=" * 60)
    print(f"DATABASE DIAGNOSTICS: {db_path.name}")
    print("=" * 60)

    # File size
    size_mb = db_path.stat().st_size / (1024 * 1024)
    print(f"File size: {size_mb:.1f} MB")

    # Check for WAL file
    wal_path = db_path.with_suffix('.duckdb.wal')
    if wal_path.exists():
        wal_size = wal_path.stat().st_size / (1024 * 1024)
        print(f"WAL file exists: {wal_size:.1f} MB (data may not be fully committed!)")
    else:
        print("WAL file: None (good - all data committed)")

    print()
    print("=" * 60)
    print("ACCOUNT SUMMARY")
    print("=" * 60)

    result = conn.execute("""
        SELECT
            account_id,
            COUNT(*) as total_rows,
            COUNT(DISTINCT video_id) as unique_videos,
            MIN(date)::VARCHAR as min_date,
            MAX(date)::VARCHAR as max_date
        FROM daily_analytics
        GROUP BY account_id
        ORDER BY total_rows DESC
    """).fetchall()

    # Load account config to map IDs to names
    config_path = script_dir.parent / 'config' / 'accounts.json'
    id_to_name = {}
    if config_path.exists():
        import json
        with open(config_path) as f:
            config = json.load(f)
        for name, acc in config.get('accounts', {}).items():
            id_to_name[str(acc.get('account_id', ''))] = name

    print(f"{'Account':<25} {'ID':<15} {'Rows':>12} {'Videos':>8} {'Date Range'}")
    print("-" * 85)
    for row in result:
        acc_id, total_rows, videos, min_date, max_date = row
        name = id_to_name.get(str(acc_id), '???')
        print(f"{name:<25} {acc_id:<15} {total_rows:>12,} {videos:>8,} {min_date} to {max_date}")

    print()
    print("=" * 60)
    print("YEAR BREAKDOWN BY ACCOUNT")
    print("=" * 60)

    result = conn.execute("""
        SELECT
            account_id,
            EXTRACT(YEAR FROM date)::INTEGER as year,
            COUNT(*) as rows,
            COUNT(DISTINCT video_id) as videos
        FROM daily_analytics
        GROUP BY account_id, year
        ORDER BY account_id, year
    """).fetchall()

    current_account = None
    for row in result:
        acc_id, year, rows, videos = row
        if acc_id != current_account:
            name = id_to_name.get(str(acc_id), '???')
            print(f"\n{name} ({acc_id}):")
            current_account = acc_id
        print(f"  {year}: {rows:>10,} rows, {videos:>6,} videos")

    # Focus on specific account if requested
    if args.account:
        print()
        print("=" * 60)
        print(f"DETAILED ANALYSIS: {args.account}")
        print("=" * 60)

        # Find account ID
        target_id = None
        for acc_id, name in id_to_name.items():
            if name.lower() == args.account.lower():
                target_id = acc_id
                break

        if not target_id:
            print(f"Account '{args.account}' not found in config")
        else:
            print(f"Account ID: {target_id}")

            # Check video_max_dates for this account
            result = conn.execute("""
                SELECT video_id, MAX(date)::VARCHAR as max_date
                FROM daily_analytics
                WHERE account_id = ?
                GROUP BY video_id
                ORDER BY max_date DESC
                LIMIT 10
            """, [target_id]).fetchall()

            print(f"\nTop 10 videos by max_date:")
            for vid, max_date in result:
                print(f"  {vid}: {max_date}")

            # Check how many have 2024 complete
            result = conn.execute("""
                SELECT
                    CASE
                        WHEN MAX(date) >= '2024-12-31' THEN 'complete_2024'
                        WHEN MAX(date) >= '2024-01-01' THEN 'partial_2024'
                        ELSE 'no_2024'
                    END as status,
                    COUNT(DISTINCT video_id) as videos
                FROM daily_analytics
                WHERE account_id = ?
                GROUP BY video_id
            """, [target_id]).fetchall()

            # Aggregate the results
            status_counts = {}
            for status, count in result:
                status_counts[status] = status_counts.get(status, 0) + count

            # Re-query properly
            result = conn.execute("""
                WITH video_status AS (
                    SELECT
                        video_id,
                        MAX(date) as max_date
                    FROM daily_analytics
                    WHERE account_id = ?
                    GROUP BY video_id
                )
                SELECT
                    CASE
                        WHEN max_date >= '2024-12-31' THEN 'complete_2024'
                        WHEN max_date >= '2024-01-01' THEN 'partial_2024'
                        ELSE 'no_2024'
                    END as status,
                    COUNT(*) as videos
                FROM video_status
                GROUP BY status
            """, [target_id]).fetchall()

            print(f"\n2024 data completeness:")
            for status, count in result:
                print(f"  {status}: {count:,} videos")

            # For partial_2024 videos, show when their last activity was
            result = conn.execute("""
                WITH video_status AS (
                    SELECT video_id, MAX(date) as max_date, MIN(date) as min_date
                    FROM daily_analytics
                    WHERE account_id = ?
                    GROUP BY video_id
                    HAVING MAX(date) < '2024-12-31' AND MAX(date) >= '2024-01-01'
                )
                SELECT
                    CASE
                        WHEN max_date >= '2024-12-01' THEN '2024-12 (Dec)'
                        WHEN max_date >= '2024-11-01' THEN '2024-11 (Nov)'
                        WHEN max_date >= '2024-10-01' THEN '2024-10 (Oct)'
                        WHEN max_date >= '2024-07-01' THEN '2024 Q3 (Jul-Sep)'
                        WHEN max_date >= '2024-04-01' THEN '2024 Q2 (Apr-Jun)'
                        ELSE '2024 Q1 (Jan-Mar)'
                    END as last_activity,
                    COUNT(*) as videos
                FROM video_status
                GROUP BY last_activity
                ORDER BY last_activity DESC
            """, [target_id]).fetchall()

            if result:
                print(f"\nPartial 2024 videos - last activity month:")
                for row in result:
                    print(f"  {row[0]}: {row[1]:,} videos")

            # Also check: how many videos in CMS but NOT in DB?
            cms_path = script_dir.parent / 'output' / 'analytics' / f'{args.account}_cms_enriched.json'
            if cms_path.exists():
                import json
                with open(cms_path) as f:
                    cms_videos = json.load(f)

                # Get all video IDs from DB for this account
                db_video_ids = set(row[0] for row in conn.execute(
                    "SELECT DISTINCT video_id FROM daily_analytics WHERE account_id = ?",
                    [target_id]
                ).fetchall())

                cms_video_ids = set(str(v.get('id')) for v in cms_videos)

                in_cms_not_db = cms_video_ids - db_video_ids
                in_db_not_cms = db_video_ids - cms_video_ids

                print(f"\nCMS vs DB comparison:")
                print(f"  Videos in CMS file: {len(cms_video_ids):,}")
                print(f"  Videos in DB: {len(db_video_ids):,}")
                print(f"  In CMS but NOT in DB: {len(in_cms_not_db):,}")
                print(f"  In DB but NOT in CMS: {len(in_db_not_cms):,}")

                if in_cms_not_db:
                    print(f"\n  Sample videos in CMS but not DB (first 5):")
                    for vid in list(in_cms_not_db)[:5]:
                        print(f"    {vid}")

            # Sample raw keys
            print(f"\nSample (account_id, video_id) keys from DB:")
            result = conn.execute("""
                SELECT DISTINCT account_id, video_id
                FROM daily_analytics
                WHERE account_id = ?
                LIMIT 5
            """, [target_id]).fetchall()
            for acc, vid in result:
                print(f"  ({repr(acc)}, {repr(vid)})")

    conn.close()
    print()
    print("Diagnostics complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
