"""
4_combine_output.py - Generate final CSV outputs from DuckDB

Purpose:
    Reads daily analytics from DuckDB and generates CSV outputs
    grouped by year and business category.

Runtime: ~1-3 minutes

Features:
    - Reads directly from DuckDB (output/analytics.duckdb)
    - Generates separate CSVs by category and by year
    - Proper column ordering matching Reporting + Harper fields
    - Supports --account flag for account-specific DuckDB files

Input:
    - output/analytics.duckdb (from script 3)
    - OR output/analytics_{account}.duckdb (with --account flag)
    - config/accounts.json (for category grouping)

Output:
    - output/daily/daily_analytics_2024_all.csv
    - output/daily/daily_analytics_2025_all.csv
    - output/daily/daily_analytics_2026_all.csv
    - output/daily/daily_analytics_combined_all.csv
    - Plus category-specific files for each year
"""

import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List
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

SCRIPT_NAME = "4_combine_output"

# Output CSV fields (same order as 3_daily_analytics.py, without data_type)
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
    "report_generated_on"
]


# =============================================================================
# CSV GENERATION
# =============================================================================

def write_csv(rows: List[Dict], output_path: Path, logger) -> int:
    """
    Write rows to CSV file.

    Returns number of rows written.
    """
    if not rows:
        logger.info(f"No rows to write for {output_path.name}")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction='ignore')
        writer.writeheader()

        for row in rows:
            # Ensure all fields exist
            clean_row = {field: row.get(field, "") for field in OUTPUT_FIELDS}
            writer.writerow(clean_row)

    logger.info(f"Written {len(rows)} rows to {output_path}")
    return len(rows)


def extract_year_from_date(date_val) -> str:
    """Extract year from date value (string or date object)."""
    if not date_val:
        return "unknown"
    date_str = str(date_val)
    if len(date_str) >= 4:
        return date_str[:4]
    return "unknown"


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate final CSV outputs from DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script reads analytics data from DuckDB and generates CSV outputs.

Features:
    - Reads directly from DuckDB (no JSONL files needed)
    - Generates separate CSVs by category and by year
    - Proper column ordering matching Reporting + Harper fields

Output:
    - output/daily/daily_analytics_YYYY_all.csv (per year)
    - output/daily/daily_analytics_YYYY_{category}.csv (per category)
    - output/daily/daily_analytics_combined_all.csv
        """
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Use account-specific DuckDB file (analytics_{account}.duckdb)'
    )
    return parser.parse_args()


def main():
    # Parse arguments (enables --help)
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting output generation (DuckDB source)")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    categories = config['accounts'].get('categories', {})

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

    # Open DuckDB connection
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

    # Load all rows from DuckDB
    logger.info("\nLoading data from DuckDB...")
    result = conn.execute("""
        SELECT * FROM daily_analytics
        ORDER BY account_id, video_id, date
    """).fetchdf()

    conn.close()

    # Convert DataFrame to list of dicts for CSV writing
    all_rows = result.to_dict('records')

    # Convert date objects to strings for CSV output
    for row in all_rows:
        if row.get("date") is not None:
            row["date"] = str(row["date"])[:10]  # YYYY-MM-DD

    logger.info(f"Total rows: {len(all_rows)}")

    # Group rows by year and category
    rows_by_year = defaultdict(list)
    rows_by_year_category = defaultdict(lambda: defaultdict(list))

    for row in all_rows:
        date = row.get("date", "")
        year = extract_year_from_date(date)
        channel = row.get("channel", "")
        category = channel_to_category.get(channel, "other")

        rows_by_year[year].append(row)
        rows_by_year_category[year][category].append(row)

    output_dir = paths['daily']

    # Write per-year files
    logger.info("\n--- Per-Year Output Files ---")
    for year in sorted(rows_by_year.keys()):
        year_rows = rows_by_year[year]

        # All channels for this year
        output_path = output_dir / f"daily_analytics_{year}_all.csv"
        write_csv(year_rows, output_path, logger)

        # Per-category for this year
        for category_name in sorted(rows_by_year_category[year].keys()):
            cat_rows = rows_by_year_category[year][category_name]
            if cat_rows:
                output_path = output_dir / f"daily_analytics_{year}_{category_name}.csv"
                write_csv(cat_rows, output_path, logger)

    # Write combined file (all years)
    logger.info("\n--- Combined Output Files ---")
    years_str = "_".join(sorted(rows_by_year.keys()))

    # All data combined
    output_path = output_dir / f"daily_analytics_{years_str}_all.csv"
    write_csv(all_rows, output_path, logger)

    # Per-category combined
    rows_by_category = defaultdict(list)
    for row in all_rows:
        channel = row.get("channel", "")
        category = channel_to_category.get(channel, "other")
        rows_by_category[category].append(row)

    for category_name in sorted(rows_by_category.keys()):
        cat_rows = rows_by_category[category_name]
        if cat_rows:
            output_path = output_dir / f"daily_analytics_{years_str}_{category_name}.csv"
            write_csv(cat_rows, output_path, logger)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Output generation completed")
    logger.info("=" * 60)

    logger.info("\nSummary by year:")
    for year in sorted(rows_by_year.keys()):
        count = len(rows_by_year[year])
        logger.info(f"  {year}: {count:,} rows")

    logger.info("\nSummary by category:")
    for category in sorted(rows_by_category.keys()):
        count = len(rows_by_category[category])
        channels = set(r.get("channel") for r in rows_by_category[category])
        logger.info(f"  {category}: {count:,} rows ({', '.join(sorted(filter(None, channels)))})")

    logger.info(f"\nTotal: {len(all_rows):,} rows")
    logger.info(f"\nOutput directory: {output_dir}")


if __name__ == "__main__":
    main()
