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

Input files (searched in checkpoints/):
    - daily_historical.jsonl (2024+2025 data)
    - daily_historical_*.jsonl (account-specific historical)
    - daily_current.jsonl (current year data)

Output:
    - output/analytics.duckdb (central database)
"""

import sys
import json
import argparse
import gc
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
        "daily_historical.jsonl",
        "daily_historical_*.jsonl",
        "daily_current.jsonl",
    ]

    found = []
    for pattern in patterns:
        found.extend(checkpoints_dir.glob(pattern))

    # Deduplicate and sort
    return sorted(set(found))


def count_lines(file_path: Path) -> int:
    """Count lines in a file efficiently."""
    count = 0
    with open(file_path, 'rb') as f:
        for _ in f:
            count += 1
    return count


def stream_jsonl_file(file_path: Path, logger):
    """
    Stream a JSONL file line by line (generator).

    Yields parsed row dicts. Memory efficient for large files.
    """
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
                yield row
            except json.JSONDecodeError as e:
                error_count += 1
                if error_count <= 5:
                    logger.warning(f"  Line {line_count}: JSON parse error: {e}")

    if error_count > 5:
        logger.warning(f"  ... and {error_count - 5} more parse errors")


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


def migrate_file(file_path: Path, conn, logger, batch_size: int = 10000) -> int:
    """
    Migrate a single JSONL file to DuckDB using streaming batches.

    Memory-efficient: processes batch_size rows at a time.

    Returns number of rows migrated.
    """
    logger.info(f"  Parsing {file_path.name}...")

    # Count total lines for progress bar
    total_lines = count_lines(file_path)
    logger.info(f"  Total lines: {total_lines:,}")

    total_migrated = 0
    batch = []
    skipped = 0

    # Stream and process in batches
    with tqdm(total=total_lines, desc=f"  {file_path.name}", unit=" rows") as pbar:
        for row in stream_jsonl_file(file_path, logger):
            # Normalize row
            normalized = normalize_row(row)

            # Check required fields
            if normalized.get("account_id") and normalized.get("video_id") and normalized.get("date"):
                batch.append(normalized)
            else:
                skipped += 1

            pbar.update(1)

            # Process batch when full
            if len(batch) >= batch_size:
                upsert_daily_analytics(conn, batch, logger)
                total_migrated += len(batch)
                batch = []  # Clear batch, free memory

        # Process remaining rows
        if batch:
            upsert_daily_analytics(conn, batch, logger)
            total_migrated += len(batch)

    if skipped > 0:
        logger.warning(f"  {skipped:,} rows missing required fields (account_id, video_id, date)")

    return total_migrated


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
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10000,
        help='Number of rows to process per batch (default: 10000). Lower this if running out of memory.'
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
        logger.info("  - daily_historical.jsonl")
        logger.info("  - daily_historical_*.jsonl")
        logger.info("  - daily_current.jsonl")
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

    for i, file_path in enumerate(jsonl_files, 1):
        logger.info(f"\nProcessing [{i}/{len(jsonl_files)}]: {file_path.name}")

        try:
            rows = migrate_file(file_path, conn, logger, batch_size=args.batch_size)
            total_migrated += rows
            files_migrated += 1
            logger.info(f"  Migrated {rows:,} rows")

            # Delete source file only if --delete flag is set
            if args.delete and rows > 0:
                file_path.unlink()
                logger.info(f"  Deleted: {file_path.name}")

            # Force garbage collection after each file to free memory
            gc.collect()

        except Exception as e:
            logger.error(f"  Failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
