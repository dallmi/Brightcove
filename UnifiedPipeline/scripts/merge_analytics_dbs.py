"""
merge_analytics_dbs.py - Merge account-specific DuckDB files into central DB

Purpose:
    After running 3_daily_analytics.py in parallel with --account flags,
    this script merges all account-specific DuckDB files into a single
    central analytics.duckdb file.

Usage:
    python merge_analytics_dbs.py

    Optional flags:
        --keep      Keep source files after merge (default: delete)
        --dry-run   Show what would be merged without actually merging

Prerequisites:
    Run 3_daily_analytics.py with --account flag for each account first:
        python 3_daily_analytics.py --account Internet
        python 3_daily_analytics.py --account Intranet
        etc.

Output:
    - output/analytics.duckdb (central merged database)
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    setup_logging,
    get_output_paths,
    init_analytics_db,
    get_db_stats,
)

SCRIPT_NAME = "merge_analytics_dbs"


def find_account_dbs(output_dir: Path, accounts: list) -> list:
    """
    Find all account-specific DuckDB files.

    Returns list of (account_name, db_path) tuples.
    """
    found = []
    for account in accounts:
        db_path = output_dir / f"analytics_{account}.duckdb"
        if db_path.exists():
            found.append((account, db_path))
    return found


def merge_db(source_path: Path, target_conn, logger) -> int:
    """
    Merge data from source DB into target connection.

    Uses INSERT OR REPLACE for upsert semantics.

    Returns number of rows merged.
    """
    import duckdb

    # Attach source database
    source_conn = duckdb.connect(str(source_path), read_only=True)

    try:
        # Count rows in source
        row_count = source_conn.execute(
            "SELECT COUNT(*) FROM daily_analytics"
        ).fetchone()[0]

        if row_count == 0:
            logger.info(f"  Source is empty, skipping")
            return 0

        # Read all data from source
        source_data = source_conn.execute(
            "SELECT * FROM daily_analytics"
        ).fetchdf()

        # Insert into target with upsert
        # DuckDB requires explicit column listing for INSERT OR REPLACE
        columns = source_data.columns.tolist()
        col_list = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])

        # Use register to make DataFrame available
        target_conn.register("source_data", source_data)

        target_conn.execute(f"""
            INSERT OR REPLACE INTO daily_analytics ({col_list})
            SELECT * FROM source_data
        """)

        target_conn.unregister("source_data")

        return row_count

    finally:
        source_conn.close()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Merge account-specific DuckDB files into central DB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
After running 3_daily_analytics.py in parallel with --account flags,
use this script to merge all account-specific databases into one.

Example workflow:
    # Step 1: Run in parallel (separate terminals)
    python 3_daily_analytics.py --account Internet
    python 3_daily_analytics.py --account Intranet
    python 3_daily_analytics.py --account Harper

    # Step 2: Merge into central DB
    python merge_analytics_dbs.py

    # Optional: Keep source files
    python merge_analytics_dbs.py --keep
        """
    )
    parser.add_argument(
        '--keep',
        action='store_true',
        help='Keep account-specific DB files after merge (default: delete)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be merged without actually merging'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting DuckDB merge operation")
    logger.info("=" * 60)

    # Load config to get account list
    config = load_config()
    accounts = list(config['accounts']['accounts'].keys())

    # Find account-specific DBs
    account_dbs = find_account_dbs(paths['output'], accounts)

    if not account_dbs:
        logger.info("No account-specific DuckDB files found.")
        logger.info(f"Expected files like: analytics_Internet.duckdb")
        logger.info(f"In directory: {paths['output']}")
        return

    logger.info(f"Found {len(account_dbs)} account-specific databases:")
    for account, db_path in account_dbs:
        logger.info(f"  - {account}: {db_path.name}")

    if args.dry_run:
        logger.info("\nDry run mode - no changes will be made")
        return

    # Initialize or open central DB
    central_db_path = paths['output'] / "analytics.duckdb"
    logger.info(f"\nTarget: {central_db_path}")

    conn = init_analytics_db(central_db_path)

    # Get initial stats
    initial_stats = get_db_stats(conn)
    logger.info(f"Initial rows in central DB: {initial_stats['total_rows']:,}")

    # Merge each account DB
    total_merged = 0
    for account, source_path in account_dbs:
        logger.info(f"\nMerging {account}...")

        try:
            rows = merge_db(source_path, conn, logger)
            total_merged += rows
            logger.info(f"  Merged {rows:,} rows from {account}")

            # Delete source file unless --keep
            if not args.keep:
                source_path.unlink()
                # Also delete WAL file if exists
                wal_path = source_path.with_suffix('.duckdb.wal')
                if wal_path.exists():
                    wal_path.unlink()
                logger.info(f"  Deleted source file: {source_path.name}")

        except Exception as e:
            logger.error(f"  Failed to merge {account}: {e}")
            continue

    # Get final stats
    final_stats = get_db_stats(conn)
    conn.close()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Merge completed")
    logger.info("=" * 60)
    logger.info(f"Accounts merged: {len(account_dbs)}")
    logger.info(f"Rows processed: {total_merged:,}")
    logger.info(f"Final total rows: {final_stats['total_rows']:,}")
    logger.info(f"Unique videos: {final_stats['unique_videos']:,}")
    logger.info(f"Date range: {final_stats['date_range'][0]} to {final_stats['date_range'][1]}")
    logger.info(f"\nOutput: {central_db_path}")


if __name__ == "__main__":
    main()
