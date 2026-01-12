"""
4_combine_output.py - Generate final CSV outputs from checkpoints

Purpose:
    Combines historical (2024+2025) and current (2026) checkpoint data
    into final CSV outputs grouped by business category.

Runtime: ~2-5 minutes

Features:
    - Merges historical + current checkpoint files
    - Generates separate CSVs by category and by year
    - Proper column ordering matching Reporting + Harper fields

Input:
    - checkpoints/daily_historical.jsonl (2024 + 2025 data)
    - checkpoints/daily_current.jsonl (2026 data)
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
    load_checkpoint_jsonl,
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


def extract_year_from_date(date_str: str) -> str:
    """Extract year from date string."""
    if not date_str:
        return "unknown"
    return date_str[:4]


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting output generation")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    categories = config['accounts'].get('categories', {})

    # Build channel -> category mapping
    channel_to_category = {}
    for category_name, category_config in categories.items():
        for account in category_config.get('accounts', []):
            channel_to_category[account] = category_name

    # Load both checkpoint files
    historical_path = paths['checkpoints'] / "daily_historical.jsonl"
    current_path = paths['checkpoints'] / "daily_current.jsonl"

    all_rows = []

    if historical_path.exists():
        historical_rows = load_checkpoint_jsonl(historical_path)
        logger.info(f"Loaded {len(historical_rows)} historical rows")
        all_rows.extend(historical_rows)
    else:
        logger.info("No historical checkpoint found")

    if current_path.exists():
        current_rows = load_checkpoint_jsonl(current_path)
        logger.info(f"Loaded {len(current_rows)} current year rows")
        all_rows.extend(current_rows)
    else:
        logger.info("No current year checkpoint found")

    if not all_rows:
        logger.warning("No data found in any checkpoint")
        return

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
        logger.info(f"  {category}: {count:,} rows ({', '.join(sorted(channels))})")

    logger.info(f"\nTotal: {len(all_rows):,} rows")
    logger.info(f"\nOutput directory: {output_dir}")


if __name__ == "__main__":
    main()
