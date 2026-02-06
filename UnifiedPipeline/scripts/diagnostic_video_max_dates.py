"""
Diagnostic script to check video_max_dates and understand why historical data is being reprocessed.

Usage:
    python diagnostic_video_max_dates.py
    python diagnostic_video_max_dates.py --account Internet
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    setup_logging,
    get_output_paths,
    init_analytics_db,
    get_all_video_max_dates,
)
import json


def analyze_video_coverage(args):
    """Analyze which videos are in DuckDB vs CMS data."""
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], "diagnostic_video_max_dates")

    # Determine DB path
    if args.account:
        db_path = paths['output'] / f"analytics_{args.account}.duckdb"
    else:
        db_path = paths['output'] / "analytics.duckdb"

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return

    logger.info(f"Analyzing database: {db_path}")

    # Check if WAL file exists
    wal_path = Path(str(db_path) + ".wal")
    if wal_path.exists():
        wal_size_mb = wal_path.stat().st_size / (1024 * 1024)
        db_size_mb = db_path.stat().st_size / (1024 * 1024)
        logger.info(f"WAL file exists: {wal_size_mb:.1f} MB")
        logger.info(f"Main DB size: {db_size_mb:.1f} MB")
        logger.warning("⚠️  WAL file present - changes not yet checkpointed!")

    # Load config
    config = load_config()
    accounts = config['accounts']['accounts']

    # Filter to single account if specified
    if args.account:
        if args.account not in accounts:
            logger.error(f"Account '{args.account}' not found")
            return
        accounts = {args.account: accounts[args.account]}

    # Connect to DuckDB
    conn = init_analytics_db(db_path)
    video_max_dates = get_all_video_max_dates(conn)

    logger.info(f"\n{'='*70}")
    logger.info(f"Total unique videos in DuckDB: {len(video_max_dates):,}")
    logger.info(f"{'='*70}")

    # Analyze by account
    total_cms_videos = 0
    total_missing_from_db = 0

    for account_name, account_config in accounts.items():
        account_id = str(account_config['account_id'])

        # Load CMS data
        cms_path = paths['analytics'] / f"{account_name}_cms_enriched.json"
        if not cms_path.exists():
            logger.warning(f"CMS data not found: {cms_path}")
            continue

        with open(cms_path, 'r', encoding='utf-8') as f:
            cms_videos = json.load(f)

        total_cms_videos += len(cms_videos)

        # Check which videos are in DuckDB
        cms_video_ids = {str(v['id']) for v in cms_videos}
        db_video_ids = {vid_id for (acc_id, vid_id) in video_max_dates.keys() if acc_id == account_id}

        missing_from_db = cms_video_ids - db_video_ids
        only_in_db = db_video_ids - cms_video_ids

        total_missing_from_db += len(missing_from_db)

        logger.info(f"\n{account_name} (Account ID: {account_id}):")
        logger.info(f"  Videos in CMS:           {len(cms_videos):,}")
        logger.info(f"  Videos in DuckDB:        {len(db_video_ids):,}")
        logger.info(f"  Missing from DuckDB:     {len(missing_from_db):,} ⚠️")
        logger.info(f"  Only in DuckDB:          {len(only_in_db):,}")

        if missing_from_db:
            logger.info(f"\n  🔍 Sample of missing video IDs (first 10):")
            for vid_id in list(missing_from_db)[:10]:
                # Find video details
                video = next((v for v in cms_videos if str(v['id']) == vid_id), None)
                if video:
                    created_at = video.get('created_at', '')[:10]
                    name = video.get('name', 'N/A')[:50]
                    logger.info(f"     {vid_id} - Created: {created_at} - {name}")

        if only_in_db:
            logger.info(f"\n  ℹ️  Videos in DB but not in CMS: {len(only_in_db)} (possibly deleted)")

    logger.info(f"\n{'='*70}")
    logger.info(f"SUMMARY:")
    logger.info(f"  Total videos in CMS:        {total_cms_videos:,}")
    logger.info(f"  Total videos in DuckDB:     {len(video_max_dates):,}")
    logger.info(f"  Missing from DuckDB:        {total_missing_from_db:,}")
    logger.info(f"{'='*70}")

    if total_missing_from_db > 0:
        logger.warning(f"\n⚠️  {total_missing_from_db} videos need historical data processing!")
        logger.info(f"This explains why the pipeline is fetching 2024-2025 data again.")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose video_max_dates coverage"
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Check specific account only'
    )
    args = parser.parse_args()

    analyze_video_coverage(args)


if __name__ == "__main__":
    main()
