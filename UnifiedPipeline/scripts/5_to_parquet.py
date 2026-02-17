"""
5_to_parquet.py - Convert DuckDB analytics data to Parquet format for PowerBI

Purpose:
    Reads daily analytics from DuckDB and exports to Parquet format with:
    - Year partitioning (faster queries)
    - Proper data types (Int64, Float, DateTime)
    - Snappy compression (~70-80% smaller than CSV)
    - Incremental mode: only regenerates current year by default

Runtime: ~1-3 minutes

Features:
    - Reads directly from DuckDB (output/analytics.duckdb)
    - Partitioned output: year=2024/, year=2025/, year=2026/
    - Incremental: skips historical years if parquet exists (use --force to override)
    - Supports --account flag for account-specific DuckDB files

Input:
    - output/analytics.duckdb (from script 3)
    - OR output/analytics_{account}.duckdb (with --account flag)
    - config/accounts.json (for category grouping)

Output:
    - output/parquet/dimensions/video_metadata.parquet (dimension table - one row per video)
    - output/parquet/facts/daily_analytics/ (partitioned by year)
    - output/parquet/facts/daily_analytics_all.parquet (single file)
    - output/parquet/facts/daily_analytics_{year}.parquet (per year)
    - output/parquet/facts/daily_analytics_{category}.parquet (per category)

Star Schema for PowerBI:
    - Fact table: daily_analytics (metrics per video per day)
      -> Only contains: video_id, date, year, metrics, original_filename
      -> Does NOT contain: video_duration, name, channel, account_id, etc.
    - Dimension table: video_metadata (one row per video with duration, name, etc.)
      -> Contains: video_id, video_duration, video_duration_seconds, name, channel, etc.
    - Join on: video_id
    - This prevents SUM(video_duration) issues when aggregating

Usage:
    python 5_to_parquet.py              # Incremental: only current year
    python 5_to_parquet.py --force      # Full: regenerate everything
    python 5_to_parquet.py --account Internet  # Use account-specific DB

Requirements:
    pip install pandas pyarrow duckdb
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set
from collections import defaultdict

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    setup_logging,
    get_output_paths,
    init_analytics_db,
    get_db_stats,
)

# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_NAME = "5_to_parquet"

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
    df,
    output_dir: Path,
    partition_col: str,
    logger
) -> None:
    """
    Write DataFrame to partitioned Parquet dataset.

    Creates: output_dir/partition_col=value/data.parquet
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

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
    df,
    output_path: Path,
    logger
) -> None:
    """Write DataFrame to single Parquet file."""
    if df is None or len(df) == 0:
        logger.warning(f"No rows to write for {output_path.name}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    # Report size
    size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(f"Written {len(df):,} rows to {output_path.name} ({size_mb:.1f} MB)")


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


def process_duckdb(
    conn,
    output_dir: Path,
    channel_to_category: Dict[str, str],
    historical_years: Set[int],
    current_year: int,
    force: bool,
    logger
) -> Dict[str, int]:
    """
    Read data from DuckDB and write Parquet outputs.

    Args:
        conn: DuckDB connection
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

    # Load data from DuckDB
    logger.info("\nLoading data from DuckDB...")
    df = conn.execute("""
        SELECT *, EXTRACT(YEAR FROM date) AS year
        FROM daily_analytics
        ORDER BY account_id, video_id, date
    """).fetchdf()

    logger.info(f"Loaded {len(df):,} rows from DuckDB")

    if len(df) == 0:
        logger.warning("No data in DuckDB")
        return {}

    # Add derived fields
    logger.info("Adding derived fields...")

    # video_duration_seconds (convert from milliseconds)
    df["video_duration_seconds"] = pd.to_numeric(df["video_duration"], errors="coerce") / 1000.0

    # Apply data types
    df = apply_dtypes(df, logger)

    # Build video metadata dimension table (one row per video, keep most recent)
    logger.info("\nBuilding video metadata dimension table...")
    dimension_cols = [c for c in DIMENSION_FIELDS if c in df.columns]
    # Sort by date descending so first occurrence per video_id is the most recent
    metadata_df = df.sort_values("date", ascending=False).drop_duplicates(
        subset=["video_id"], keep="first"
    )[dimension_cols].copy()

    logger.info(f"Unique videos for dimension table: {len(metadata_df):,}")

    # Separate fact table columns
    fact_cols = [c for c in FACT_FIELDS if c in df.columns]

    results = {}

    # Filter out years to skip for fact table processing
    if years_to_skip:
        process_df = df[~df["year"].isin(years_to_skip)].copy()
        skipped_count = len(df) - len(process_df)
        logger.info(f"Skipped {skipped_count:,} rows from existing historical years")
    else:
        process_df = df

    fact_df = process_df[fact_cols].copy()

    logger.info(f"Total fact rows to write: {len(fact_df):,}")

    # 1. Write partitioned dataset (by year) - only for years we processed
    logger.info("\n--- Writing Partitioned Dataset (by year) ---")
    partitioned_dir = facts_dir / "daily_analytics"
    write_parquet_partitioned(fact_df, partitioned_dir, "year", logger)
    results["facts/daily_analytics (partitioned)"] = len(fact_df)

    # 2. Write per-year files (only for years we processed)
    logger.info("\n--- Writing Per-Year Files ---")
    for year, year_df in fact_df.groupby("year"):
        year_path = facts_dir / f"daily_analytics_{int(year)}.parquet"
        write_parquet_single(year_df, year_path, logger)
        results[f"facts/daily_analytics_{int(year)}.parquet"] = len(year_df)

    # 3. Write per-category files (only include processed data)
    logger.info("\n--- Writing Per-Category Files ---")
    process_df["category"] = process_df["channel"].map(
        lambda ch: channel_to_category.get(ch, "other")
    )
    for category, cat_df in process_df[fact_cols + ["category"]].groupby("category"):
        cat_fact_df = cat_df[fact_cols].copy()
        cat_path = facts_dir / f"daily_analytics_{category}.parquet"
        write_parquet_single(cat_fact_df, cat_path, logger)
        results[f"facts/daily_analytics_{category}.parquet"] = len(cat_fact_df)

    # 4. Write combined file - needs special handling for incremental mode
    logger.info("\n--- Writing Combined File ---")
    if years_to_skip:
        # In incremental mode, merge with existing historical parquet files
        logger.info("Merging new data with existing historical parquet files...")
        combined_parts = [fact_df]

        for year in sorted(years_to_skip):
            existing_path = facts_dir / f"daily_analytics_{int(year)}.parquet"
            if existing_path.exists():
                existing_df = pd.read_parquet(existing_path)
                combined_parts.append(existing_df)
                logger.info(f"  Loaded {len(existing_df):,} rows from existing {year} file")

        combined_df = pd.concat(combined_parts, ignore_index=True)
        combined_path = facts_dir / "daily_analytics_all.parquet"
        write_parquet_single(combined_df, combined_path, logger)
        results["facts/daily_analytics_all.parquet"] = len(combined_df)
    else:
        combined_path = facts_dir / "daily_analytics_all.parquet"
        write_parquet_single(fact_df, combined_path, logger)
        results["facts/daily_analytics_all.parquet"] = len(fact_df)

    # 5. Write video metadata dimension table (always regenerated)
    logger.info("\n--- Writing Video Metadata Dimension Table ---")
    metadata_path = dimensions_dir / "video_metadata.parquet"
    write_parquet_single(metadata_df, metadata_path, logger)
    results["dimensions/video_metadata.parquet"] = len(metadata_df)
    logger.info(f"  -> Use this table for video_duration (one row per video)")
    logger.info(f"  -> Join with daily_analytics on video_id")

    return results


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert DuckDB analytics data to Parquet format for PowerBI"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration of all parquet files (including historical)"
    )
    parser.add_argument(
        "--account",
        type=str,
        help="Use account-specific DuckDB file (analytics_{account}.duckdb)"
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
        import duckdb
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install pandas pyarrow duckdb")
        sys.exit(1)

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting Parquet conversion (DuckDB source)")
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

    # Determine DuckDB path
    if args.account:
        db_path = paths['output'] / f"analytics_{args.account}.duckdb"
    else:
        db_path = paths['output'] / "analytics.duckdb"

    logger.info(f"DuckDB source: {db_path}")

    if not db_path.exists():
        logger.error(f"DuckDB file not found: {db_path}")
        logger.error("Run script 3 (3_daily_analytics.py) first to populate the database.")
        return

    # Open DuckDB connection (read-only)
    conn = init_analytics_db(db_path)

    # Show DB stats
    stats = get_db_stats(conn)
    logger.info(f"DuckDB stats:")
    logger.info(f"  Total rows: {stats['total_rows']:,}")
    logger.info(f"  Unique videos: {stats['unique_videos']:,}")
    if stats['date_range'][0]:
        logger.info(f"  Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")

    if stats['total_rows'] == 0:
        logger.error("DuckDB is empty. Run script 3 first.")
        conn.close()
        return

    # Process and write
    results = process_duckdb(
        conn=conn,
        output_dir=paths['output'],
        channel_to_category=channel_to_category,
        historical_years=historical_years,
        current_year=current_year,
        force=args.force,
        logger=logger
    )

    conn.close()

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
        logger.info("     -> Contains: video_metadata.parquet (one row per video)")
        logger.info("     -> Fields: video_duration_seconds, name, channel, account_id, etc.")
        logger.info("  2. Import from Folder: facts/")
        logger.info("     -> Contains: daily_analytics files (metrics per video per day)")
        logger.info("     -> Fields: video_id, date, video_view, engagement metrics, etc.")
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
