"""
sync_all.py - Master sync script for CrossPlatformAnalytics

This script orchestrates syncing data from both Vbrick and Brightcove
to the unified cross-platform database.

Usage:
    python sync_all.py                # Sync all data
    python sync_all.py --stats        # Show statistics only
    python sync_all.py --dry-run      # Preview without writing
    python sync_all.py --vbrick-only  # Sync Vbrick only
    python sync_all.py --brightcove-only  # Sync Brightcove only
"""

import argparse
import logging
import sys
from datetime import datetime

from shared_crossplatform import (
    init_crossplatform_db,
    get_vbrick_db_path,
    get_brightcove_db_path,
    get_crossplatform_db_path,
    print_db_stats,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def check_source_databases() -> dict:
    """
    Check which source databases are available.

    Returns:
        Dict with availability status
    """
    return {
        'vbrick': get_vbrick_db_path().exists(),
        'brightcove': get_brightcove_db_path().exists(),
    }


def run_vbrick_sync(dry_run: bool = False) -> dict:
    """
    Run Vbrick sync and return statistics.

    Args:
        dry_run: If True, don't write to database

    Returns:
        Dict with sync statistics
    """
    import duckdb
    from shared_crossplatform import (
        upsert_video_daily,
        upsert_webcasts,
        upsert_accounts,
        map_vbrick_video_row,
        map_vbrick_webcast_row,
    )
    from tqdm import tqdm

    stats = {'video_rows': 0, 'webcast_rows': 0, 'accounts': 0}

    vbrick_db_path = get_vbrick_db_path()
    if not vbrick_db_path.exists():
        logger.warning(f"Vbrick database not found at {vbrick_db_path}")
        return stats

    source_conn = duckdb.connect(str(vbrick_db_path), read_only=True)
    target_conn = init_crossplatform_db()

    try:
        # Sync account
        if not dry_run:
            account_row = {
                'platform': 'vbrick',
                'account_id': 'vbrick',
                'account_name': 'Vbrick (Internal Video Platform)',
                'account_category': 'internal',
                'is_active': True
            }
            upsert_accounts(target_conn, [account_row], logger)
        stats['accounts'] = 1

        # Sync video data
        result = source_conn.execute("SELECT * FROM vbrick_video_daily ORDER BY video_id, date").fetchall()
        columns = [desc[0] for desc in source_conn.description]

        if not dry_run:
            batch_size = 1000
            for i in tqdm(range(0, len(result), batch_size), desc="Vbrick videos"):
                batch_rows = result[i:i + batch_size]
                mapped_rows = [map_vbrick_video_row(dict(zip(columns, row))) for row in batch_rows]
                upsert_video_daily(target_conn, mapped_rows, logger)
        stats['video_rows'] = len(result)

        # Sync webcasts
        result = source_conn.execute("SELECT * FROM vbrick_webcasts ORDER BY start_date").fetchall()
        columns = [desc[0] for desc in source_conn.description]

        if not dry_run:
            mapped_rows = [map_vbrick_webcast_row(dict(zip(columns, row))) for row in result]
            upsert_webcasts(target_conn, mapped_rows, logger)
        stats['webcast_rows'] = len(result)

    finally:
        source_conn.close()
        target_conn.close()

    return stats


def run_brightcove_sync(dry_run: bool = False) -> dict:
    """
    Run Brightcove sync and return statistics.

    Args:
        dry_run: If True, don't write to database

    Returns:
        Dict with sync statistics
    """
    import duckdb
    import json
    from shared_crossplatform import (
        upsert_video_daily,
        upsert_accounts,
        map_brightcove_video_row,
        get_project_root,
    )
    from tqdm import tqdm

    stats = {'video_rows': 0, 'accounts': 0}

    brightcove_db_path = get_brightcove_db_path()
    if not brightcove_db_path.exists():
        logger.warning(f"Brightcove database not found at {brightcove_db_path}")
        return stats

    source_conn = duckdb.connect(str(brightcove_db_path), read_only=True)
    target_conn = init_crossplatform_db()

    try:
        # Sync accounts from config
        accounts_path = get_project_root().parent / "UnifiedPipeline" / "config" / "accounts.json"
        if accounts_path.exists():
            with open(accounts_path) as f:
                config = json.load(f)
            accounts = config.get('accounts', {})

            if not dry_run:
                account_rows = [
                    {
                        'platform': 'brightcove',
                        'account_id': info.get('account_id'),
                        'account_name': name,
                        'account_category': info.get('category'),
                        'is_active': True
                    }
                    for name, info in accounts.items()
                ]
                upsert_accounts(target_conn, account_rows, logger)
            stats['accounts'] = len(accounts)

        # Sync video data
        result = source_conn.execute("SELECT * FROM daily_analytics ORDER BY account_id, video_id, date").fetchall()
        columns = [desc[0] for desc in source_conn.description]

        if not dry_run:
            batch_size = 1000
            for i in tqdm(range(0, len(result), batch_size), desc="Brightcove videos"):
                batch_rows = result[i:i + batch_size]
                mapped_rows = [map_brightcove_video_row(dict(zip(columns, row))) for row in batch_rows]
                upsert_video_daily(target_conn, mapped_rows, logger)
        stats['video_rows'] = len(result)

    finally:
        source_conn.close()
        target_conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description='Sync all data to CrossPlatformAnalytics')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--vbrick-only', action='store_true', help='Sync Vbrick only')
    parser.add_argument('--brightcove-only', action='store_true', help='Sync Brightcove only')
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("CrossPlatformAnalytics - Master Sync")
    logger.info("=" * 60)

    # Check available sources
    available = check_source_databases()
    logger.info("\nSource Database Status:")
    logger.info(f"  Vbrick: {'Available' if available['vbrick'] else 'Not found'} ({get_vbrick_db_path()})")
    logger.info(f"  Brightcove: {'Available' if available['brightcove'] else 'Not found'} ({get_brightcove_db_path()})")

    if args.stats:
        # Show stats only
        logger.info("\nCross-Platform Database Statistics:")
        conn = init_crossplatform_db()
        print_db_stats(conn, logger)
        conn.close()
        return

    if not available['vbrick'] and not available['brightcove']:
        logger.error("\nNo source databases found. Run the individual pipelines first:")
        logger.error("  - Vbrick: python 01_fetch_analytics.py && python 02_Webcast.py")
        logger.error("  - Brightcove: python 3_daily_analytics.py")
        sys.exit(1)

    # Determine what to sync
    sync_vbrick = available['vbrick'] and not args.brightcove_only
    sync_brightcove = available['brightcove'] and not args.vbrick_only

    total_stats = {
        'vbrick': {'video_rows': 0, 'webcast_rows': 0, 'accounts': 0},
        'brightcove': {'video_rows': 0, 'accounts': 0}
    }

    # Run syncs
    if sync_vbrick:
        logger.info("\n" + "-" * 40)
        logger.info("Syncing Vbrick data...")
        logger.info("-" * 40)
        total_stats['vbrick'] = run_vbrick_sync(args.dry_run)

    if sync_brightcove:
        logger.info("\n" + "-" * 40)
        logger.info("Syncing Brightcove data...")
        logger.info("-" * 40)
        total_stats['brightcove'] = run_brightcove_sync(args.dry_run)

    # Summary
    elapsed = datetime.now() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("SYNC COMPLETE")
    logger.info("=" * 60)

    if sync_vbrick:
        vb = total_stats['vbrick']
        logger.info(f"\nVbrick:")
        logger.info(f"  Accounts: {vb['accounts']}")
        logger.info(f"  Video daily rows: {vb['video_rows']:,}")
        logger.info(f"  Webcasts: {vb['webcast_rows']:,}")

    if sync_brightcove:
        bc = total_stats['brightcove']
        logger.info(f"\nBrightcove:")
        logger.info(f"  Accounts: {bc['accounts']}")
        logger.info(f"  Video daily rows: {bc['video_rows']:,}")

    total_video_rows = total_stats['vbrick']['video_rows'] + total_stats['brightcove']['video_rows']
    logger.info(f"\nTotal video daily rows: {total_video_rows:,}")
    logger.info(f"Total webcasts: {total_stats['vbrick']['webcast_rows']:,}")
    logger.info(f"Elapsed time: {elapsed}")

    if args.dry_run:
        logger.info("\n(DRY RUN - no data was written)")

    # Show final database stats
    if not args.dry_run:
        logger.info("\n" + "-" * 40)
        logger.info("Final Database Statistics:")
        logger.info("-" * 40)
        conn = init_crossplatform_db()
        print_db_stats(conn, logger)
        conn.close()

    logger.info(f"\nDatabase location: {get_crossplatform_db_path()}")


if __name__ == "__main__":
    main()
