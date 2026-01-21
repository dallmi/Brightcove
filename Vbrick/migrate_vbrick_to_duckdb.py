"""
migrate_vbrick_to_duckdb.py - Migration script for existing Vbrick CSV data

This script migrates existing CSV files to the DuckDB database.
It handles both video analytics and webcast summary files.

Usage:
    python migrate_vbrick_to_duckdb.py                    # Migrate all CSV files
    python migrate_vbrick_to_duckdb.py --dry-run          # Preview without writing
    python migrate_vbrick_to_duckdb.py --video-csv FILE   # Migrate specific video CSV
    python migrate_vbrick_to_duckdb.py --webcast-csv FILE # Migrate specific webcast CSV
    python migrate_vbrick_to_duckdb.py --stats            # Show database statistics
"""

import argparse
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from tqdm import tqdm

from shared_vbrick import (
    init_vbrick_db,
    upsert_video_daily,
    upsert_webcasts,
    get_db_stats,
    print_db_stats,
    get_output_dir,
    get_vbrick_db_path,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_csv_files(directory: Path, pattern: str) -> List[Path]:
    """Find CSV files matching a pattern in the directory."""
    files = list(directory.glob(pattern))
    return sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)


def parse_video_csv(csv_path: Path) -> List[Dict[str, Any]]:
    """
    Parse a video analytics CSV file into rows for DuckDB.

    Maps CSV column names to DuckDB schema column names.
    """
    rows = []
    report_date = datetime.now().isoformat()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for csv_row in reader:
            row = {
                'video_id': csv_row.get('video_id'),
                'date': csv_row.get('date'),
                'title': csv_row.get('title'),
                'playback_url': csv_row.get('playbackUrl'),
                'duration': safe_int(csv_row.get('duration')),
                'when_uploaded': csv_row.get('whenUploaded'),
                'last_viewed': csv_row.get('lastViewed'),
                'when_published': csv_row.get('whenPublished'),
                'uploaded_by': csv_row.get('uploadedBy'),
                'tags': csv_row.get('tags'),
                'comment_count': safe_int(csv_row.get('commentCount')),
                'score': safe_float(csv_row.get('score')),
                'views': safe_int(csv_row.get('views')),
                'device_desktop': safe_int(csv_row.get('Desktop')),
                'device_mobile': safe_int(csv_row.get('Mobile')),
                'device_other': safe_int(csv_row.get('Other Device')),
                'browser_chrome': safe_int(csv_row.get('Chrome')),
                'browser_edge': safe_int(csv_row.get('Microsoft Edge')),
                'browser_other': safe_int(csv_row.get('Other Browser')),
                'report_generated_on': report_date,
            }

            # Skip rows without required fields
            if row['video_id'] and row['date']:
                rows.append(row)

    return rows


def parse_webcast_csv(csv_path: Path) -> List[Dict[str, Any]]:
    """
    Parse a webcast summary CSV file into rows for DuckDB.

    Maps CSV column names to DuckDB schema column names.
    """
    rows = []
    report_date = datetime.now().isoformat()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for csv_row in reader:
            row = {
                'event_id': csv_row.get('id'),
                'title': csv_row.get('title'),
                'vod_id': csv_row.get('vodId'),
                'event_url': csv_row.get('eventUrl'),
                'start_date': csv_row.get('startDate'),
                'end_date': csv_row.get('endDate'),
                'attendee_count': safe_int(csv_row.get('attendeeCount')),
                'attendee_total': safe_int(csv_row.get('attendeeTotal')),
                'total_viewing_time': safe_int(csv_row.get('total_viewingTime')),
                'zone_apac': safe_int(csv_row.get('zone_APAC')),
                'zone_america': safe_int(csv_row.get('zone_America')),
                'zone_emea': safe_int(csv_row.get('zone_EMEA')),
                'zone_swiss': safe_int(csv_row.get('zone_Swiss')),
                'zone_other': safe_int(csv_row.get('zone_Other')),
                'browser_chrome': safe_int(csv_row.get('browser_Chrome')),
                'browser_edge': safe_int(csv_row.get('browser_Edge')),
                'browser_other': safe_int(csv_row.get('browser_Other')),
                'device_pc': safe_int(csv_row.get('deviceType_PC')),
                'device_mobile': safe_int(csv_row.get('deviceType_Mobile')),
                'device_other': safe_int(csv_row.get('deviceType_Other')),
                'category': csv_row.get('category'),
                'subcategory': csv_row.get('subcategory'),
                'report_generated_on': report_date,
            }

            # Skip rows without required fields
            if row['event_id']:
                rows.append(row)

    return rows


def safe_int(value: Optional[str]) -> Optional[int]:
    """Safely convert a string to int, returning None on failure."""
    if value is None or value == '':
        return None
    try:
        # Handle European number format (comma as decimal separator)
        value = str(value).replace(',', '.')
        return int(float(value))
    except (ValueError, TypeError):
        return None


def safe_float(value: Optional[str]) -> Optional[float]:
    """Safely convert a string to float, returning None on failure."""
    if value is None or value == '':
        return None
    try:
        # Handle European number format (comma as decimal separator)
        value = str(value).replace(',', '.')
        return float(value)
    except (ValueError, TypeError):
        return None


def migrate_video_csv(csv_path: Path, conn, dry_run: bool = False) -> int:
    """
    Migrate a video analytics CSV file to DuckDB.

    Args:
        csv_path: Path to CSV file
        conn: DuckDB connection
        dry_run: If True, don't write to database

    Returns:
        Number of rows migrated
    """
    logger.info(f"Parsing video CSV: {csv_path}")
    rows = parse_video_csv(csv_path)

    if not rows:
        logger.warning(f"No valid rows found in {csv_path}")
        return 0

    logger.info(f"Found {len(rows)} rows to migrate")

    if dry_run:
        logger.info("[DRY RUN] Would upsert {len(rows)} rows to vbrick_video_daily")
        return len(rows)

    # Batch upsert
    batch_size = 1000
    total_upserted = 0

    for i in tqdm(range(0, len(rows), batch_size), desc="Migrating video data"):
        batch = rows[i:i + batch_size]
        upserted = upsert_video_daily(conn, batch, logger)
        total_upserted += upserted

    logger.info(f"Migrated {total_upserted} video analytics rows")
    return total_upserted


def migrate_webcast_csv(csv_path: Path, conn, dry_run: bool = False) -> int:
    """
    Migrate a webcast summary CSV file to DuckDB.

    Args:
        csv_path: Path to CSV file
        conn: DuckDB connection
        dry_run: If True, don't write to database

    Returns:
        Number of rows migrated
    """
    logger.info(f"Parsing webcast CSV: {csv_path}")
    rows = parse_webcast_csv(csv_path)

    if not rows:
        logger.warning(f"No valid rows found in {csv_path}")
        return 0

    logger.info(f"Found {len(rows)} rows to migrate")

    if dry_run:
        logger.info(f"[DRY RUN] Would upsert {len(rows)} rows to vbrick_webcasts")
        return len(rows)

    # Upsert all rows
    upserted = upsert_webcasts(conn, rows, logger)
    logger.info(f"Migrated {upserted} webcast rows")
    return upserted


def main():
    parser = argparse.ArgumentParser(description='Migrate Vbrick CSV data to DuckDB')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing to database')
    parser.add_argument('--stats', action='store_true', help='Show database statistics and exit')
    parser.add_argument('--video-csv', type=str, help='Path to specific video analytics CSV file')
    parser.add_argument('--webcast-csv', type=str, help='Path to specific webcast summary CSV file')
    parser.add_argument('--input-dir', type=str, help='Directory containing CSV files to migrate')
    args = parser.parse_args()

    # Show stats and exit if requested
    if args.stats:
        print_db_stats(logger=logger)
        return

    # Initialize DuckDB
    conn = init_vbrick_db()
    logger.info(f"Initialized DuckDB at {get_vbrick_db_path()}")

    # Determine input directory
    if args.input_dir:
        input_dir = Path(args.input_dir)
    else:
        input_dir = get_output_dir()

    total_video_rows = 0
    total_webcast_rows = 0

    # Migrate specific video CSV or find all
    if args.video_csv:
        video_path = Path(args.video_csv)
        if video_path.exists():
            total_video_rows = migrate_video_csv(video_path, conn, args.dry_run)
        else:
            logger.error(f"Video CSV not found: {video_path}")
    else:
        # Find all video analytics CSVs
        video_files = find_csv_files(input_dir, "vbrick_analytics_*.csv")
        if not video_files:
            # Try legacy naming pattern
            video_files = find_csv_files(input_dir, "*_TV_*.csv")

        if video_files:
            logger.info(f"Found {len(video_files)} video CSV file(s)")
            # Only migrate the most recent one to avoid duplicates
            for video_file in video_files[:1]:
                total_video_rows += migrate_video_csv(video_file, conn, args.dry_run)
        else:
            logger.info("No video analytics CSV files found")

    # Migrate specific webcast CSV or find all
    if args.webcast_csv:
        webcast_path = Path(args.webcast_csv)
        if webcast_path.exists():
            total_webcast_rows = migrate_webcast_csv(webcast_path, conn, args.dry_run)
        else:
            logger.error(f"Webcast CSV not found: {webcast_path}")
    else:
        # Find all webcast summary CSVs
        webcast_files = find_csv_files(input_dir, "webcast_summary*.csv")

        if webcast_files:
            logger.info(f"Found {len(webcast_files)} webcast CSV file(s)")
            # Only migrate the most recent one
            for webcast_file in webcast_files[:1]:
                total_webcast_rows += migrate_webcast_csv(webcast_file, conn, args.dry_run)
        else:
            logger.info("No webcast summary CSV files found")

    # Close connection
    conn.close()

    # Summary
    logger.info("=" * 60)
    logger.info("Migration Summary:")
    logger.info(f"  Video analytics rows: {total_video_rows:,}")
    logger.info(f"  Webcast rows: {total_webcast_rows:,}")
    if args.dry_run:
        logger.info("  (DRY RUN - no data was written)")
    logger.info("=" * 60)

    # Print final stats
    if not args.dry_run:
        print_db_stats(logger=logger)


if __name__ == "__main__":
    main()
