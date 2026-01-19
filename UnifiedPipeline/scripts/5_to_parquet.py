"""
5_to_parquet.py - Convert checkpoint data to Parquet format for PowerBI

Purpose:
    Converts JSONL checkpoint files directly to Parquet format with:
    - Streaming processing (low memory usage)
    - Year partitioning (faster queries)
    - Proper data types (Int64, Float, DateTime)
    - Snappy compression (~70-80% smaller than CSV)
    - Incremental mode: only regenerates current year by default

Runtime: ~2-5 minutes (full), ~30 seconds (incremental)

Features:
    - Reads JSONL checkpoints directly (skips CSV generation)
    - Streaming: processes in batches to handle multi-GB files
    - Partitioned output: year=2024/, year=2025/, year=2026/
    - Incremental: skips historical years if parquet exists (use --force to override)

Input:
    - checkpoints/daily_historical.jsonl (2024 + 2025 data)
    - checkpoints/daily_current.jsonl (2026 data)
    - config/accounts.json (for category grouping)

Output:
    - output/parquet/dimensions/video_metadata.parquet (dimension table - one row per video)
    - output/parquet/facts/daily_analytics/ (partitioned by year)
    - output/parquet/facts/daily_analytics_all.parquet (single file)
    - output/parquet/facts/daily_analytics_{year}.parquet (per year)
    - output/parquet/facts/daily_analytics_{category}.parquet (per category)

Star Schema for PowerBI:
    - Fact table: daily_analytics (metrics per video per day)
      → Only contains: video_id, date, year, metrics, original_filename
      → Does NOT contain: video_duration, name, channel, account_id, etc.
    - Dimension table: video_metadata (one row per video with duration, name, etc.)
      → Contains: video_id, video_duration, video_duration_seconds, name, channel, etc.
    - Join on: video_id
    - This prevents SUM(video_duration) issues when aggregating

Usage:
    python 5_to_parquet.py          # Incremental: only current year
    python 5_to_parquet.py --force  # Full: regenerate everything

Requirements:
    pip install pandas pyarrow
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Iterator, Optional, Set
from collections import defaultdict

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    setup_logging,
    get_output_paths,
)

# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_NAME = "5_to_parquet"

# Batch size for streaming (number of rows to accumulate before writing)
BATCH_SIZE = 50000

# Output fields (same as 4_combine_output.py)
OUTPUT_FIELDS = [
    # Core identifiers
    "channel", "account_id", "video_id", "name", "date",
    # View metrics
    "video_view", "views_desktop", "views_mobile", "views_tablet", "views_other",
    # Engagement metrics
    "video_impression", "play_rate", "engagement_score",
    "video_engagement_1", "video_engagement_25", "video_engagement_50",
    "video_engagement_75", "video_engagement_100",
    "video_percent_viewed", "video_seconds_viewed",
    # CMS metadata
    "created_at", "published_at", "original_filename", "created_by",
    # Custom fields (standard)
    "video_content_type", "video_length", "video_duration", "video_category",
    "country", "language", "business_unit",
    "tags", "reference_id",
    # Harper additions
    "dt_last_viewed",
    "cf_relatedlinkname", "cf_relatedlink", "cf_video_owner_email",
    "cf_1a_comms_sign_off", "cf_1b_comms_sign_off_approver",
    "cf_2a_data_classification_disclaimer", "cf_3a_records_management_disclaimer",
    "cf_4a_archiving_disclaimer_comms_branding", "cf_4b_unique_sharepoint_id",
    # Meta
    "report_generated_on",
    # Derived (added by this script)
    "year",
]

# Dimension table fields (video metadata - one row per video)
DIMENSION_FIELDS = [
    "video_id", "account_id", "channel", "name",
    "video_duration", "video_duration_seconds",  # Both ms and seconds
    "created_at", "published_at", "original_filename", "created_by",
    "video_content_type", "video_length", "video_category",
    "country", "language", "business_unit",
    "tags", "reference_id",
    # Harper custom fields
    "cf_relatedlinkname", "cf_relatedlink", "cf_video_owner_email",
    "cf_1a_comms_sign_off", "cf_1b_comms_sign_off_approver",
    "cf_2a_data_classification_disclaimer", "cf_3a_records_management_disclaimer",
    "cf_4a_archiving_disclaimer_comms_branding", "cf_4b_unique_sharepoint_id",
]

# Fact table fields (daily metrics - many rows per video)
# Note: Metadata fields are in video_metadata.parquet, join via video_id
FACT_FIELDS = [
    "video_id",  # Join key to video_metadata
    "date", "year",
    # Metrics (change daily)
    "video_view", "views_desktop", "views_mobile", "views_tablet", "views_other",
    "video_impression", "play_rate", "engagement_score",
    "video_engagement_1", "video_engagement_25", "video_engagement_50",
    "video_engagement_75", "video_engagement_100",
    "video_percent_viewed", "video_seconds_viewed",
    # Kept for convenience (user request)
    "original_filename",
    # Meta
    "dt_last_viewed", "report_generated_on",
]

# Column data types for Parquet
COLUMN_DTYPES = {
    # String columns
    "channel": "string",
    "account_id": "string",
    "video_id": "string",
    "name": "string",
    "original_filename": "string",
    "created_by": "string",
    "video_content_type": "string",
    "video_length": "string",
    "video_category": "string",
    "country": "string",
    "language": "string",
    "business_unit": "string",
    "tags": "string",
    "reference_id": "string",
    "cf_relatedlinkname": "string",
    "cf_relatedlink": "string",
    "cf_video_owner_email": "string",
    "cf_1a_comms_sign_off": "string",
    "cf_1b_comms_sign_off_approver": "string",
    "cf_2a_data_classification_disclaimer": "string",
    "cf_3a_records_management_disclaimer": "string",
    "cf_4a_archiving_disclaimer_comms_branding": "string",
    "cf_4b_unique_sharepoint_id": "string",

    # Date columns
    "date": "datetime64[ns]",
    "created_at": "datetime64[ns]",
    "published_at": "datetime64[ns]",
    "dt_last_viewed": "datetime64[ns]",
    "report_generated_on": "datetime64[ns]",

    # Integer columns (nullable)
    "video_view": "Int64",
    "views_desktop": "Int64",
    "views_mobile": "Int64",
    "views_tablet": "Int64",
    "views_other": "Int64",
    "video_impression": "Int64",
    "video_seconds_viewed": "Int64",
    "video_duration": "Int64",  # Duration in milliseconds (from API)
    "year": "Int64",

    # Float columns (derived)
    "video_duration_seconds": "float64",  # Duration in seconds (for convenience)

    # Float columns
    "play_rate": "float64",
    "engagement_score": "float64",
    "video_engagement_1": "float64",
    "video_engagement_25": "float64",
    "video_engagement_50": "float64",
    "video_engagement_75": "float64",
    "video_engagement_100": "float64",
    "video_percent_viewed": "float64",
}


# =============================================================================
# STREAMING JSONL READER
# =============================================================================

def stream_jsonl(file_path: Path, logger) -> Iterator[Dict]:
    """
    Stream JSONL file line by line (memory efficient).

    Yields one row dict at a time.
    """
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return

    line_count = 0
    error_count = 0

    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                row = json.loads(line)
                line_count += 1
                yield row
            except json.JSONDecodeError as e:
                error_count += 1
                if error_count <= 5:
                    logger.warning(f"JSON parse error at line {line_count}: {e}")

    logger.info(f"Streamed {line_count} rows from {file_path.name} ({error_count} errors)")


def count_jsonl_lines(file_path: Path) -> int:
    """Count lines in JSONL file (for progress tracking)."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        for _ in f:
            count += 1
    return count


# =============================================================================
# DATA PROCESSING
# =============================================================================

def extract_year(date_str: str) -> Optional[int]:
    """Extract year from date string."""
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


def process_row(row: Dict) -> Dict:
    """
    Process a single row: add derived fields, ensure all columns exist.
    """
    # Add year column
    date_str = row.get("date", "")
    row["year"] = extract_year(date_str)

    # Add video_duration_seconds (convert from milliseconds)
    duration_ms = row.get("video_duration")
    if duration_ms is not None:
        try:
            row["video_duration_seconds"] = float(duration_ms) / 1000.0
        except (TypeError, ValueError):
            row["video_duration_seconds"] = None
    else:
        row["video_duration_seconds"] = None

    # Ensure all expected fields exist
    for field in OUTPUT_FIELDS:
        if field not in row:
            row[field] = None

    return row


def extract_dimension_row(row: Dict) -> Dict:
    """Extract dimension table fields from a row."""
    return {field: row.get(field) for field in DIMENSION_FIELDS}


def extract_fact_row(row: Dict) -> Dict:
    """Extract fact table fields from a row."""
    return {field: row.get(field) for field in FACT_FIELDS}


# =============================================================================
# PARQUET WRITING
# =============================================================================

def apply_dtypes(df, logger):
    """Apply proper data types to DataFrame columns."""
    import pandas as pd

    for col, dtype in COLUMN_DTYPES.items():
        if col not in df.columns:
            continue

        try:
            if "datetime" in dtype:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "string":
                df[col] = df[col].astype("string")
            elif dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        except Exception as e:
            logger.warning(f"Could not convert {col} to {dtype}: {e}")

    return df


def write_parquet_partitioned(
    rows: List[Dict],
    output_dir: Path,
    partition_col: str,
    logger
) -> None:
    """
    Write rows to partitioned Parquet dataset.

    Creates: output_dir/partition_col=value/data.parquet
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = pd.DataFrame(rows)
    df = apply_dtypes(df, logger)

    # Ensure partition column exists and is not null
    if partition_col not in df.columns:
        logger.error(f"Partition column {partition_col} not found")
        return

    # Remove rows with null partition values
    df = df.dropna(subset=[partition_col])

    if len(df) == 0:
        logger.warning("No valid rows to write after filtering")
        return

    # Convert to PyArrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write partitioned dataset
    output_dir.mkdir(parents=True, exist_ok=True)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=[partition_col],
        compression="snappy",
        existing_data_behavior="overwrite_or_ignore"
    )

    logger.info(f"Written {len(df)} rows to partitioned dataset: {output_dir}")


def write_parquet_single(
    rows: List[Dict],
    output_path: Path,
    logger
) -> None:
    """Write rows to single Parquet file."""
    import pandas as pd

    if not rows:
        logger.warning(f"No rows to write for {output_path.name}")
        return

    df = pd.DataFrame(rows)
    df = apply_dtypes(df, logger)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    # Report size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"Written {len(rows):,} rows to {output_path.name} ({size_mb:.1f} MB)")


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def get_existing_parquet_years(parquet_dir: Path) -> Set[int]:
    """Check which year parquet files already exist."""
    existing_years = set()
    for f in parquet_dir.glob("daily_analytics_*.parquet"):
        # Extract year from filename like "daily_analytics_2024.parquet"
        name = f.stem  # "daily_analytics_2024"
        parts = name.split("_")
        if len(parts) >= 3:
            try:
                year = int(parts[-1])
                if 2000 <= year <= 2100:  # Sanity check
                    existing_years.add(year)
            except ValueError:
                pass
    return existing_years


def process_checkpoints_streaming(
    checkpoint_files: List[Path],
    output_dir: Path,
    channel_to_category: Dict[str, str],
    historical_years: Set[int],
    current_year: int,
    force: bool,
    logger
) -> Dict[str, int]:
    """
    Process checkpoint files with streaming and write Parquet outputs.

    Args:
        checkpoint_files: List of JSONL checkpoint files to process
        output_dir: Root output directory
        channel_to_category: Mapping of channel name to category
        historical_years: Set of historical years (e.g., {2024, 2025})
        current_year: Current year (e.g., 2026)
        force: If True, regenerate all files. If False, skip existing historical.
        logger: Logger instance

    Returns:
        Dict of output file -> row count
    """
    import pandas as pd

    parquet_dir = output_dir / "parquet"
    facts_dir = parquet_dir / "facts"
    dimensions_dir = parquet_dir / "dimensions"
    facts_dir.mkdir(parents=True, exist_ok=True)
    dimensions_dir.mkdir(parents=True, exist_ok=True)

    # Check which historical years already have parquet files
    existing_years = get_existing_parquet_years(facts_dir)

    # Determine which years to skip
    if force:
        years_to_skip = set()
        logger.info("FORCE MODE: Regenerating all parquet files")
    else:
        years_to_skip = existing_years & historical_years
        if years_to_skip:
            logger.info(f"INCREMENTAL MODE: Skipping existing historical years: {sorted(years_to_skip)}")
        else:
            logger.info("INCREMENTAL MODE: No existing historical files found, processing all")

    # Count total lines for progress
    total_lines = sum(count_jsonl_lines(f) for f in checkpoint_files)
    logger.info(f"Total rows in checkpoints: {total_lines:,}")

    # Accumulators for batched writing
    all_rows = []
    rows_by_year = defaultdict(list)
    rows_by_category = defaultdict(list)

    # Video metadata accumulator (keyed by video_id, keeps most recent)
    video_metadata = {}

    processed = 0
    skipped = 0

    # Stream through all checkpoint files
    for checkpoint_file in checkpoint_files:
        logger.info(f"\nProcessing: {checkpoint_file.name}")

        for row in stream_jsonl(checkpoint_file, logger):
            row = process_row(row)

            year = row.get("year")
            channel = row.get("channel", "")
            category = channel_to_category.get(channel, "other")
            video_id = row.get("video_id")

            # Always update video metadata (regardless of year skipping)
            # This ensures dimension table has all videos
            if video_id:
                video_metadata[video_id] = extract_dimension_row(row)

            # Skip rows from years we don't need to regenerate
            if year in years_to_skip:
                skipped += 1
                continue

            # Extract only fact fields for fact table outputs
            fact_row = extract_fact_row(row)
            all_rows.append(fact_row)
            if year:
                rows_by_year[year].append(fact_row)
            rows_by_category[category].append(fact_row)

            processed += 1

            # Progress logging every 100k rows
            if processed % 100000 == 0:
                logger.info(f"  Processed {processed:,} rows (skipped {skipped:,} from existing years)")

    logger.info(f"\nTotal rows to write: {processed:,} (skipped {skipped:,} from existing historical)")
    logger.info(f"Unique videos found: {len(video_metadata):,}")

    if not all_rows and not video_metadata:
        logger.warning("No data to write")
        return {}

    results = {}

    # 1. Write partitioned dataset (by year) - only for years we processed
    logger.info("\n--- Writing Partitioned Dataset (by year) ---")
    partitioned_dir = facts_dir / "daily_analytics"
    write_parquet_partitioned(all_rows, partitioned_dir, "year", logger)
    results["facts/daily_analytics (partitioned)"] = len(all_rows)

    # 2. Write per-year files (only for years we processed)
    logger.info("\n--- Writing Per-Year Files ---")
    for year in sorted(rows_by_year.keys()):
        year_rows = rows_by_year[year]
        year_path = facts_dir / f"daily_analytics_{year}.parquet"
        write_parquet_single(year_rows, year_path, logger)
        results[f"facts/daily_analytics_{year}.parquet"] = len(year_rows)

    # 3. Write per-category files (only include processed data)
    logger.info("\n--- Writing Per-Category Files ---")
    for category in sorted(rows_by_category.keys()):
        cat_rows = rows_by_category[category]
        cat_path = facts_dir / f"daily_analytics_{category}.parquet"
        write_parquet_single(cat_rows, cat_path, logger)
        results[f"facts/daily_analytics_{category}.parquet"] = len(cat_rows)

    # 4. Write combined file - needs special handling for incremental mode
    logger.info("\n--- Writing Combined File ---")
    if years_to_skip:
        # In incremental mode, we need to merge with existing data
        logger.info("Merging new data with existing historical parquet files...")
        combined_rows = list(all_rows)  # Start with newly processed rows

        # Load existing parquet files for skipped years
        for year in sorted(years_to_skip):
            existing_path = facts_dir / f"daily_analytics_{year}.parquet"
            if existing_path.exists():
                existing_df = pd.read_parquet(existing_path)
                existing_records = existing_df.to_dict('records')
                combined_rows.extend(existing_records)
                logger.info(f"  Loaded {len(existing_records):,} rows from existing {year} file")

        combined_path = facts_dir / "daily_analytics_all.parquet"
        write_parquet_single(combined_rows, combined_path, logger)
        results["facts/daily_analytics_all.parquet"] = len(combined_rows)
    else:
        # Full mode - just write all processed rows
        combined_path = facts_dir / "daily_analytics_all.parquet"
        write_parquet_single(all_rows, combined_path, logger)
        results["facts/daily_analytics_all.parquet"] = len(all_rows)

    # 5. Write video metadata dimension table (always regenerated)
    logger.info("\n--- Writing Video Metadata Dimension Table ---")
    if video_metadata:
        metadata_rows = list(video_metadata.values())
        metadata_path = dimensions_dir / "video_metadata.parquet"
        write_parquet_single(metadata_rows, metadata_path, logger)
        results["dimensions/video_metadata.parquet"] = len(metadata_rows)
        logger.info(f"  → Use this table for video_duration (one row per video)")
        logger.info(f"  → Join with daily_analytics on video_id")

    return results


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert checkpoint data to Parquet format for PowerBI"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration of all parquet files (including historical)"
    )
    return parser.parse_args()


def main():
    # Parse arguments
    args = parse_args()

    # Check dependencies
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install pandas pyarrow")
        sys.exit(1)

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting Parquet conversion")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    settings = config['settings']
    categories = config['accounts'].get('categories', {})

    # Get historical years and current year from settings
    historical_years = set(settings.get('years', {}).get('historical', [2024, 2025]))
    current_year = settings.get('years', {}).get('current', datetime.now().year)

    logger.info(f"Historical years: {sorted(historical_years)}")
    logger.info(f"Current year: {current_year}")
    logger.info(f"Force mode: {args.force}")

    # Build channel -> category mapping
    channel_to_category = {}
    for category_name, category_config in categories.items():
        for account in category_config.get('accounts', []):
            channel_to_category[account] = category_name

    # Find checkpoint files
    checkpoint_files = []

    historical_path = paths['checkpoints'] / "daily_historical.jsonl"
    current_path = paths['checkpoints'] / "daily_current.jsonl"

    if historical_path.exists():
        checkpoint_files.append(historical_path)
        size_mb = historical_path.stat().st_size / (1024 * 1024)
        logger.info(f"Found historical checkpoint: {size_mb:.1f} MB")
    else:
        logger.warning("No historical checkpoint found")

    if current_path.exists():
        checkpoint_files.append(current_path)
        size_mb = current_path.stat().st_size / (1024 * 1024)
        logger.info(f"Found current checkpoint: {size_mb:.1f} MB")
    else:
        logger.warning("No current checkpoint found")

    if not checkpoint_files:
        logger.error("No checkpoint files found. Run scripts 1-3 first.")
        return

    # Process and write
    results = process_checkpoints_streaming(
        checkpoint_files=checkpoint_files,
        output_dir=paths['output'],
        channel_to_category=channel_to_category,
        historical_years=historical_years,
        current_year=current_year,
        force=args.force,
        logger=logger
    )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Parquet conversion completed!")
    logger.info("=" * 60)

    if results:
        logger.info("\nOutput files:")
        for filename, count in results.items():
            logger.info(f"  {filename}: {count:,} rows")

        # Calculate total size
        parquet_dir = paths['output'] / "parquet"
        total_size = sum(f.stat().st_size for f in parquet_dir.rglob("*.parquet"))
        total_size_mb = total_size / (1024 * 1024)

        logger.info(f"\nTotal Parquet size: {total_size_mb:.1f} MB")
        logger.info(f"Output directory: {parquet_dir}")

        logger.info("\n" + "-" * 60)
        logger.info("PowerBI Import Instructions:")
        logger.info("-" * 60)
        logger.info("")
        logger.info("RECOMMENDED: Star Schema (prevents SUM aggregation issues)")
        logger.info("  1. Import from Folder: dimensions/")
        logger.info("     → Contains: video_metadata.parquet (one row per video)")
        logger.info("     → Fields: video_duration_seconds, name, channel, account_id, etc.")
        logger.info("  2. Import from Folder: facts/")
        logger.info("     → Contains: daily_analytics files (metrics per video per day)")
        logger.info("     → Fields: video_id, date, video_view, engagement metrics, etc.")
        logger.info("  3. Create relationship: video_id (many-to-one)")
        logger.info("  4. Use fields from Dimension Table for name, duration, channel, etc.")
        logger.info("")
        logger.info("Folder structure for PowerBI 'Import from Folder':")
        logger.info(f"  - Dimensions: {parquet_dir}/dimensions/")
        logger.info(f"  - Facts:      {parquet_dir}/facts/")
    else:
        logger.info("No files were written.")


if __name__ == "__main__":
    main()
