"""
sync_brightcove.py - Sync Brightcove data to CrossPlatformAnalytics database

This script reads data from the Brightcove/UnifiedPipeline DuckDB database
and syncs it to the cross-platform unified database.

Usage:
    python sync_brightcove.py           # Sync all Brightcove data
    python sync_brightcove.py --stats   # Show statistics only
    python sync_brightcove.py --dry-run # Preview without writing
"""

import argparse
import json
import logging
from pathlib import Path
from tqdm import tqdm

from shared_crossplatform import (
    init_crossplatform_db,
    get_brightcove_db_path,
    get_project_root,
    upsert_video_daily,
    upsert_accounts,
    map_brightcove_video_row,
    print_db_stats,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_brightcove_accounts() -> dict:
    """
    Load Brightcove account configuration from UnifiedPipeline.

    Returns:
        Dict with account configurations
    """
    accounts_path = get_project_root().parent / "UnifiedPipeline" / "config" / "accounts.json"

    if not accounts_path.exists():
        logger.warning(f"Accounts config not found at {accounts_path}")
        return {}

    with open(accounts_path) as f:
        return json.load(f)


def sync_brightcove_accounts(target_conn, dry_run: bool = False) -> int:
    """
    Sync Brightcove accounts to dim_accounts.

    Args:
        target_conn: Connection to cross-platform database
        dry_run: If True, don't write to database

    Returns:
        Number of accounts synced
    """
    config = load_brightcove_accounts()
    accounts = config.get('accounts', {})

    if not accounts:
        logger.warning("No Brightcove accounts found in config")
        return 0

    logger.info(f"Found {len(accounts)} Brightcove accounts to sync")

    if dry_run:
        logger.info("[DRY RUN] Would sync Brightcove accounts")
        return len(accounts)

    account_rows = []
    for name, info in accounts.items():
        account_rows.append({
            'platform': 'brightcove',
            'account_id': info.get('account_id'),
            'account_name': name,
            'account_category': info.get('category'),
            'is_active': True
        })

    upsert_accounts(target_conn, account_rows, logger)
    logger.info(f"Synced {len(account_rows)} Brightcove accounts to dim_accounts")
    return len(account_rows)


def sync_brightcove_videos(source_conn, target_conn, dry_run: bool = False) -> int:
    """
    Sync video analytics from Brightcove to cross-platform database.

    Args:
        source_conn: Connection to Brightcove database
        target_conn: Connection to cross-platform database
        dry_run: If True, don't write to database

    Returns:
        Number of rows synced
    """
    logger.info("Fetching video data from Brightcove...")

    # Get all rows from daily_analytics
    result = source_conn.execute("""
        SELECT * FROM daily_analytics
        ORDER BY account_id, video_id, date
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
            mapped_row = map_brightcove_video_row(row_dict)
            mapped_rows.append(mapped_row)

        # Upsert batch
        upsert_video_daily(target_conn, mapped_rows, logger)
        total_synced += len(mapped_rows)

    logger.info(f"Synced {total_synced} video daily rows from Brightcove")
    return total_synced


def get_brightcove_stats(source_conn) -> dict:
    """
    Get statistics from Brightcove source database.

    Args:
        source_conn: Connection to Brightcove database

    Returns:
        Dict with statistics
    """
    stats = {}

    # Total rows
    stats['total_rows'] = source_conn.execute(
        "SELECT COUNT(*) FROM daily_analytics"
    ).fetchone()[0]

    # By account
    result = source_conn.execute("""
        SELECT
            channel,
            COUNT(*) as row_count,
            COUNT(DISTINCT video_id) as video_count,
            SUM(video_view) as total_views
        FROM daily_analytics
        GROUP BY channel
        ORDER BY row_count DESC
    """).fetchall()

    stats['by_account'] = [
        {
            'channel': row[0],
            'row_count': row[1],
            'video_count': row[2],
            'total_views': row[3]
        }
        for row in result
    ]

    # Date range
    result = source_conn.execute("""
        SELECT MIN(date), MAX(date) FROM daily_analytics
    """).fetchone()
    stats['min_date'] = str(result[0]) if result[0] else None
    stats['max_date'] = str(result[1]) if result[1] else None

    return stats


def main():
    parser = argparse.ArgumentParser(description='Sync Brightcove data to CrossPlatformAnalytics')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    args = parser.parse_args()

    # Check source database exists
    brightcove_db_path = get_brightcove_db_path()
    if not brightcove_db_path.exists():
        logger.error(f"Brightcove database not found at {brightcove_db_path}")
        logger.error("Run the UnifiedPipeline first (3_daily_analytics.py)")
        return

    # Initialize connections
    import duckdb
    source_conn = duckdb.connect(str(brightcove_db_path), read_only=True)
    target_conn = init_crossplatform_db()

    if args.stats:
        # Show stats and exit
        logger.info("Brightcove Source Statistics:")
        stats = get_brightcove_stats(source_conn)
        logger.info(f"  Total rows: {stats['total_rows']:,}")
        logger.info(f"  Date range: {stats['min_date']} to {stats['max_date']}")
        logger.info("\n  By Account:")
        for acct in stats['by_account']:
            logger.info(f"    {acct['channel']}: {acct['row_count']:,} rows, {acct['video_count']:,} videos, {acct['total_views']:,} views")

        logger.info("\nCross-Platform Target Statistics:")
        print_db_stats(target_conn, logger)

        source_conn.close()
        target_conn.close()
        return

    try:
        # Sync accounts
        account_count = sync_brightcove_accounts(target_conn, args.dry_run)

        # Sync video daily data
        video_rows = sync_brightcove_videos(source_conn, target_conn, args.dry_run)

        # Summary
        logger.info("=" * 60)
        logger.info("Brightcove Sync Summary:")
        logger.info(f"  Accounts: {account_count}")
        logger.info(f"  Video daily rows: {video_rows:,}")
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
