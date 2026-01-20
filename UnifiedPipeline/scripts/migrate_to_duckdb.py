"""
migrate_to_duckdb.py - Migrate existing JSONL checkpoint data to DuckDB

Purpose:
    One-time migration script to convert existing JSONL checkpoint files
    from the old pipeline format to the new DuckDB-based format.

    This preserves all previously fetched analytics data while enabling
    the new incremental processing with overlap.

Usage:
    python migrate_to_duckdb.py

    Optional flags:
        --dry-run    Show what would be migrated without actually migrating
        --delete     Delete JSONL files after successful migration (default: keep)

Input files (searched in output/checkpoints/):
    - *_daily_analytics_*.jsonl (historical format)
    - *_analytics_checkpoint_*.jsonl (current year format)

Output:
    - output/analytics.duckdb (central database)
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    setup_logging,
    get_output_paths,
    init_analytics_db,
    upsert_daily_analytics,
    get_db_stats,
)

SCRIPT_NAME = "migrate_to_duckdb"


def find_jsonl_files(checkpoints_dir: Path) -> list:
    """
    Find all JSONL checkpoint files to migrate.

    Returns list of Path objects.
    """
    patterns = [
        "*_daily_analytics_*.jsonl",
        "*_analytics_checkpoint_*.jsonl",
        "daily_analytics_*.jsonl",
    ]

    found = []
    for pattern in patterns:
        found.extend(checkpoints_dir.glob(pattern))

    # Deduplicate and sort
    return sorted(set(found))


def parse_jsonl_file(file_path: Path, logger) -> list:
    """
    Parse a JSONL file and return list of row dicts.

    Handles malformed lines gracefully.
    """
    rows = []
    line_count = 0
    error_count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            line = line.strip()
            if not line:
                continue

            try:
                row = json.loads(line)
                rows.append(row)
            except json.JSONDecodeError as e:
                error_count += 1
                if error_count <= 5:
                    logger.warning(f"  Line {line_count}: JSON parse error: {e}")

    if error_count > 5:
        logger.warning(f"  ... and {error_count - 5} more parse errors")

    return rows


def normalize_row(row: dict) -> dict:
    """
    Normalize a row to match the new DuckDB schema.

    Handles differences between old and new field names.
    """
    # Map old field names to new ones
    field_mapping = {
        "video": "video_id",
        "account": "account_id",
        "data_type": "data_type",
    }

    normalized = {}

    for key, value in row.items():
        # Apply field mapping
        new_key = field_mapping.get(key, key)
        normalized[new_key] = value

    # Ensure required fields exist
    defaults = {
        "video_view": 0,
        "video_impression": 0,
        "play_rate": 0,
        "engagement_score": 0,
        "video_engagement_1": 0,
        "video_engagement_25": 0,
        "video_engagement_50": 0,
        "video_engagement_75": 0,
        "video_engagement_100": 0,
        "video_percent_viewed": 0,
        "video_seconds_viewed": 0,
        "views_desktop": 0,
        "views_mobile": 0,
        "views_tablet": 0,
        "views_other": 0,
    }

    for field, default in defaults.items():
        if field not in normalized:
            normalized[field] = default

    return normalized


def migrate_file(file_path: Path, conn, logger) -> int:
    """
    Migrate a single JSONL file to DuckDB.

    Returns number of rows migrated.
    """
    logger.info(f"  Parsing {file_path.name}...")
    rows = parse_jsonl_file(file_path, logger)

    if not rows:
        logger.info(f"  No valid rows found")
        return 0

    # Normalize rows
    normalized = [normalize_row(r) for r in rows]

    # Filter rows that have required fields
    valid_rows = [
        r for r in normalized
        if r.get("account_id") and r.get("video_id") and r.get("date")
    ]

    if len(valid_rows) != len(normalized):
        logger.warning(
            f"  {len(normalized) - len(valid_rows)} rows missing required fields (account_id, video_id, date)"
        )

    if not valid_rows:
        return 0

    # Upsert to DuckDB
    upsert_daily_analytics(conn, valid_rows, logger)

    return len(valid_rows)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Migrate existing JSONL checkpoint data to DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
One-time migration script for converting existing checkpoint data.

Example:
    # Preview what would be migrated
    python migrate_to_duckdb.py --dry-run

    # Migrate (keeps original files by default)
    python migrate_to_duckdb.py

    # Migrate and delete original files after success
    python migrate_to_duckdb.py --delete
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be migrated without actually migrating'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete JSONL files after successful migration (default: keep)'
    )
    parser.add_argument(
        '--checkpoint-dir',
        type=str,
        help='Override checkpoint directory path'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting JSONL to DuckDB migration")
    logger.info("=" * 60)

    # Find checkpoint directory
    if args.checkpoint_dir:
        checkpoints_dir = Path(args.checkpoint_dir)
    else:
        checkpoints_dir = paths['checkpoints']

    logger.info(f"Checkpoint directory: {checkpoints_dir}")

    if not checkpoints_dir.exists():
        logger.error(f"Checkpoint directory not found: {checkpoints_dir}")
        return

    # Find JSONL files
    jsonl_files = find_jsonl_files(checkpoints_dir)

    if not jsonl_files:
        logger.info("No JSONL checkpoint files found to migrate.")
        logger.info("Searched for patterns:")
        logger.info("  - *_daily_analytics_*.jsonl")
        logger.info("  - *_analytics_checkpoint_*.jsonl")
        logger.info("  - daily_analytics_*.jsonl")
        return

    logger.info(f"Found {len(jsonl_files)} JSONL files to migrate:")
    total_size = 0
    for f in jsonl_files:
        size = f.stat().st_size / (1024 * 1024)
        total_size += size
        logger.info(f"  - {f.name} ({size:.1f} MB)")

    logger.info(f"Total size: {total_size:.1f} MB")

    if args.dry_run:
        logger.info("\nDry run mode - no changes will be made")
        return

    # Initialize DuckDB
    db_path = paths['output'] / "analytics.duckdb"
    logger.info(f"\nTarget database: {db_path}")

    conn = init_analytics_db(db_path)

    # Get initial stats
    initial_stats = get_db_stats(conn)
    logger.info(f"Initial rows in DB: {initial_stats['total_rows']:,}")

    # Migrate each file
    total_migrated = 0
    files_migrated = 0

    for file_path in tqdm(jsonl_files, desc="Migrating files"):
        logger.info(f"\nProcessing: {file_path.name}")

        try:
            rows = migrate_file(file_path, conn, logger)
            total_migrated += rows
            files_migrated += 1
            logger.info(f"  Migrated {rows:,} rows")

            # Delete source file only if --delete flag is set
            if args.delete and rows > 0:
                file_path.unlink()
                logger.info(f"  Deleted: {file_path.name}")

        except Exception as e:
            logger.error(f"  Failed: {e}")
            continue

    # Get final stats
    final_stats = get_db_stats(conn)
    conn.close()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Migration completed")
    logger.info("=" * 60)
    logger.info(f"Files processed: {files_migrated}/{len(jsonl_files)}")
    logger.info(f"Rows migrated: {total_migrated:,}")
    logger.info(f"Final total rows: {final_stats['total_rows']:,}")
    logger.info(f"Unique videos: {final_stats['unique_videos']:,}")

    if final_stats['date_range'][0]:
        logger.info(f"Date range: {final_stats['date_range'][0]} to {final_stats['date_range'][1]}")

    logger.info(f"\nOutput: {db_path}")

    if args.delete and total_migrated > 0:
        logger.info("\nOriginal JSONL files have been deleted.")
    elif total_migrated > 0:
        logger.info("\nOriginal JSONL files have been preserved.")
        logger.info("Use --delete flag to remove them after verifying migration.")


if __name__ == "__main__":
    main()
