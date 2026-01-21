"""
sync_vbrick.py - Sync Vbrick data to CrossPlatformAnalytics database

This script reads data from the Vbrick DuckDB database and syncs it
to the cross-platform unified database.

Usage:
    python sync_vbrick.py           # Sync all Vbrick data
    python sync_vbrick.py --stats   # Show statistics only
    python sync_vbrick.py --dry-run # Preview without writing
"""

import argparse
import logging
from pathlib import Path
from tqdm import tqdm

from shared_crossplatform import (
    init_crossplatform_db,
    get_vbrick_db_path,
    upsert_video_daily,
    upsert_webcasts,
    upsert_accounts,
    map_vbrick_video_row,
    map_vbrick_webcast_row,
    print_db_stats,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def sync_vbrick_videos(source_conn, target_conn, dry_run: bool = False) -> int:
    """
    Sync video analytics from Vbrick to cross-platform database.

    Args:
        source_conn: Connection to Vbrick database
        target_conn: Connection to cross-platform database
        dry_run: If True, don't write to database

    Returns:
        Number of rows synced
    """
    logger.info("Fetching video data from Vbrick...")

    # Get all rows from vbrick_video_daily
    result = source_conn.execute("""
        SELECT * FROM vbrick_video_daily
        ORDER BY video_id, date
    """).fetchall()

    # Get column names
    columns = [desc[0] for desc in source_conn.description]

    logger.info(f"Found {len(result)} video daily rows to sync")

    if dry_run:
        logger.info("[DRY RUN] Would sync video data")
        return len(result)

    # Map and upsert in batches
    batch_size = 1000
    total_synced = 0

    for i in tqdm(range(0, len(result), batch_size), desc="Syncing videos"):
        batch_rows = result[i:i + batch_size]

        # Convert to dicts and map to unified schema
        mapped_rows = []
        for row in batch_rows:
            row_dict = dict(zip(columns, row))
            mapped_row = map_vbrick_video_row(row_dict)
            mapped_rows.append(mapped_row)

        # Upsert batch
        upsert_video_daily(target_conn, mapped_rows, logger)
        total_synced += len(mapped_rows)

    logger.info(f"Synced {total_synced} video daily rows from Vbrick")
    return total_synced


def sync_vbrick_webcasts(source_conn, target_conn, dry_run: bool = False) -> int:
    """
    Sync webcasts from Vbrick to cross-platform database.

    Args:
        source_conn: Connection to Vbrick database
        target_conn: Connection to cross-platform database
        dry_run: If True, don't write to database

    Returns:
        Number of rows synced
    """
    logger.info("Fetching webcast data from Vbrick...")

    # Get all rows from vbrick_webcasts
    result = source_conn.execute("""
        SELECT * FROM vbrick_webcasts
        ORDER BY start_date, event_id
    """).fetchall()

    # Get column names
    columns = [desc[0] for desc in source_conn.description]

    logger.info(f"Found {len(result)} webcasts to sync")

    if dry_run:
        logger.info("[DRY RUN] Would sync webcast data")
        return len(result)

    # Map and upsert all at once (webcasts are typically fewer)
    mapped_rows = []
    for row in result:
        row_dict = dict(zip(columns, row))
        mapped_row = map_vbrick_webcast_row(row_dict)
        mapped_rows.append(mapped_row)

    # Upsert all
    upsert_webcasts(target_conn, mapped_rows, logger)

    logger.info(f"Synced {len(mapped_rows)} webcasts from Vbrick")
    return len(mapped_rows)


def sync_vbrick_account(target_conn, dry_run: bool = False) -> int:
    """
    Add Vbrick as an account in dim_accounts.

    Args:
        target_conn: Connection to cross-platform database
        dry_run: If True, don't write to database

    Returns:
        Number of rows synced
    """
    if dry_run:
        logger.info("[DRY RUN] Would add Vbrick account")
        return 1

    account_row = {
        'platform': 'vbrick',
        'account_id': 'vbrick',
        'account_name': 'Vbrick (Internal Video Platform)',
        'account_category': 'internal',
        'is_active': True
    }

    upsert_accounts(target_conn, [account_row], logger)
    logger.info("Added Vbrick account to dim_accounts")
    return 1


def main():
    parser = argparse.ArgumentParser(description='Sync Vbrick data to CrossPlatformAnalytics')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    args = parser.parse_args()

    # Check source database exists
    vbrick_db_path = get_vbrick_db_path()
    if not vbrick_db_path.exists():
        logger.error(f"Vbrick database not found at {vbrick_db_path}")
        logger.error("Run the Vbrick pipeline first (01_fetch_analytics.py, 02_Webcast.py)")
        return

    # Initialize connections
    import duckdb
    source_conn = duckdb.connect(str(vbrick_db_path), read_only=True)
    target_conn = init_crossplatform_db()

    if args.stats:
        # Show stats and exit
        logger.info("Vbrick Source Statistics:")
        video_count = source_conn.execute("SELECT COUNT(*) FROM vbrick_video_daily").fetchone()[0]
        webcast_count = source_conn.execute("SELECT COUNT(*) FROM vbrick_webcasts").fetchone()[0]
        logger.info(f"  Video daily rows: {video_count:,}")
        logger.info(f"  Webcasts: {webcast_count:,}")

        logger.info("\nCross-Platform Target Statistics:")
        print_db_stats(target_conn, logger)

        source_conn.close()
        target_conn.close()
        return

    try:
        # Sync account
        sync_vbrick_account(target_conn, args.dry_run)

        # Sync video daily data
        video_rows = sync_vbrick_videos(source_conn, target_conn, args.dry_run)

        # Sync webcasts
        webcast_rows = sync_vbrick_webcasts(source_conn, target_conn, args.dry_run)

        # Summary
        logger.info("=" * 60)
        logger.info("Vbrick Sync Summary:")
        logger.info(f"  Video daily rows: {video_rows:,}")
        logger.info(f"  Webcasts: {webcast_rows:,}")
        if args.dry_run:
            logger.info("  (DRY RUN - no data was written)")
        logger.info("=" * 60)

        # Show target stats
        if not args.dry_run:
            print_db_stats(target_conn, logger)

    finally:
        source_conn.close()
        target_conn.close()


if __name__ == "__main__":
    main()
