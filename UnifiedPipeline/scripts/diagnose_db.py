"""
diagnose_db.py - Diagnostic tool for DuckDB analytics database

Run this to check database state and debug incremental processing issues.
"""

import sys
import argparse
from pathlib import Path

def check_wal_backup(db_path, wal_backup_path, account_name, script_dir):
    """
    Check a WAL backup file for missing videos.

    This creates a temporary copy of the DB + WAL to see what data
    would have been in the WAL before recovery.
    """
    import duckdb
    import tempfile
    import shutil
    import json

    wal_backup = Path(wal_backup_path)
    if not wal_backup.exists():
        print(f"ERROR: WAL backup not found: {wal_backup}")
        return 1

    # Load account config to get account ID
    config_path = script_dir.parent / 'config' / 'accounts.json'
    if not config_path.exists():
        print("ERROR: accounts.json not found")
        return 1

    with open(config_path) as f:
        config = json.load(f)

    target_id = None
    if account_name:
        for name, acc in config.get('accounts', {}).items():
            if name.lower() == account_name.lower():
                target_id = str(acc.get('account_id', ''))
                break
        if not target_id:
            print(f"ERROR: Account '{account_name}' not found")
            return 1

    print("=" * 60)
    print("WAL BACKUP ANALYSIS")
    print("=" * 60)
    print(f"WAL backup: {wal_backup}")
    print(f"WAL size: {wal_backup.stat().st_size / (1024*1024):.1f} MB")
    print()

    # Get missing video IDs from current DB vs CMS
    print("Loading current DB and CMS data...")
    conn_current = duckdb.connect(str(db_path), read_only=True)

    if target_id:
        db_video_ids = set(row[0] for row in conn_current.execute(
            "SELECT DISTINCT video_id FROM daily_analytics WHERE account_id = ?",
            [target_id]
        ).fetchall())
    else:
        db_video_ids = set(row[0] for row in conn_current.execute(
            "SELECT DISTINCT video_id FROM daily_analytics"
        ).fetchall())
    conn_current.close()

    # Load CMS
    cms_path = script_dir.parent / 'output' / 'analytics' / f'{account_name}_cms_enriched.json'
    if cms_path.exists():
        with open(cms_path) as f:
            cms_videos = json.load(f)
        cms_video_ids = set(str(v.get('id')) for v in cms_videos)
        missing_from_db = cms_video_ids - db_video_ids
        print(f"Videos in CMS: {len(cms_video_ids):,}")
        print(f"Videos in current DB: {len(db_video_ids):,}")
        print(f"Missing from DB: {len(missing_from_db):,}")
    else:
        print(f"CMS file not found: {cms_path}")
        missing_from_db = set()

    # Create temp directory and copy DB + WAL
    print()
    print("Creating temporary DB with WAL to analyze...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_db = Path(tmpdir) / "temp_analytics.duckdb"
        tmp_wal = Path(tmpdir) / "temp_analytics.duckdb.wal"

        # Copy the main DB file
        shutil.copy(db_path, tmp_db)
        # Copy the WAL backup as the WAL file
        shutil.copy(wal_backup, tmp_wal)

        try:
            # Open the DB - DuckDB will automatically recover the WAL
            conn_with_wal = duckdb.connect(str(tmp_db))

            if target_id:
                wal_video_ids = set(row[0] for row in conn_with_wal.execute(
                    "SELECT DISTINCT video_id FROM daily_analytics WHERE account_id = ?",
                    [target_id]
                ).fetchall())

                # Get stats
                result = conn_with_wal.execute("""
                    SELECT COUNT(*), COUNT(DISTINCT video_id), MIN(date), MAX(date)
                    FROM daily_analytics WHERE account_id = ?
                """, [target_id]).fetchone()
            else:
                wal_video_ids = set(row[0] for row in conn_with_wal.execute(
                    "SELECT DISTINCT video_id FROM daily_analytics"
                ).fetchall())

                result = conn_with_wal.execute("""
                    SELECT COUNT(*), COUNT(DISTINCT video_id), MIN(date), MAX(date)
                    FROM daily_analytics
                """).fetchone()

            conn_with_wal.close()

            print(f"\nDB + WAL recovered stats:")
            print(f"  Total rows: {result[0]:,}")
            print(f"  Unique videos: {result[1]:,}")
            print(f"  Date range: {result[2]} to {result[3]}")

            # Compare
            videos_in_wal_not_db = wal_video_ids - db_video_ids
            videos_in_db_not_wal = db_video_ids - wal_video_ids

            print(f"\nComparison (DB+WAL vs current DB):")
            print(f"  Videos in DB+WAL: {len(wal_video_ids):,}")
            print(f"  Videos in current DB: {len(db_video_ids):,}")
            print(f"  In WAL but not current DB: {len(videos_in_wal_not_db):,}")
            print(f"  In current DB but not WAL: {len(videos_in_db_not_wal):,}")

            if missing_from_db:
                # Check how many of the CMS-missing videos are in the WAL
                missing_found_in_wal = missing_from_db & wal_video_ids
                still_missing = missing_from_db - wal_video_ids

                print(f"\nOf the {len(missing_from_db):,} videos missing from current DB:")
                print(f"  Found in WAL backup: {len(missing_found_in_wal):,}")
                print(f"  NOT in WAL either: {len(still_missing):,}")

                if still_missing:
                    print(f"\n  These {len(still_missing):,} videos were never processed:")
                    for vid in list(still_missing)[:10]:
                        print(f"    {vid}")
                    if len(still_missing) > 10:
                        print(f"    ... and {len(still_missing) - 10} more")

        except Exception as e:
            print(f"ERROR recovering WAL: {e}")
            return 1

    print()
    print("WAL analysis complete.")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Diagnose DuckDB analytics database")
    parser.add_argument('--db', type=str, help='Path to DuckDB file (default: output/analytics.duckdb)')
    parser.add_argument('--account', type=str, help='Focus on specific account name')
    parser.add_argument('--check-wal-backup', type=str, help='Check WAL backup file for missing videos')
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

    # Handle --check-wal-backup mode
    if args.check_wal_backup:
        return check_wal_backup(db_path, args.check_wal_backup, args.account, script_dir)

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
