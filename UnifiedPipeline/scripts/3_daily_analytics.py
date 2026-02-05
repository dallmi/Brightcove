"""
3_daily_analytics.py - Fetch detailed daily analytics per video

Purpose:
    Fetches detailed daily analytics (views, engagement, device breakdown)
    for all videos across all configured years.

Strategy:
    - Unified approach: ALL videos for ALL years (no 90-day filter)
    - Incremental updates with 7-day overlap for Brightcove lag compensation
    - DuckDB-based checkpointing with upsert support
    - Account-specific DB files for parallel processing

Runtime:
    - First run: ~4-8 hours (all historical data)
    - Subsequent runs: ~30-60 minutes (incremental with overlap)

Run frequency: Monthly/Weekly/Daily (flexible)

Prerequisites: Scripts 1 and 2 MUST run before this script at every execution!
    - 1_cms_metadata.py captures new videos
    - 2_dt_last_viewed.py updates dt_last_viewed

Features:
    - DuckDB checkpoint for atomic upserts (handles duplicates)
    - 7-day overlap to compensate for Brightcove's 24-72h analytics lag
    - --account flag for parallel processing with separate DB files
    - Device breakdown (desktop, mobile, tablet, other)

Input:
    - output/analytics/{account}_cms_enriched.json (from script 2)
    - secrets.json (credentials)
    - config/settings.json (year configuration)

Output:
    - output/analytics.duckdb (central DB, without --account)
    - output/analytics_{account}.duckdb (account-specific, with --account)

Parallel Processing:
    To run multiple accounts in parallel, use separate terminals:
        Terminal 1: python 3_daily_analytics.py --account Internet
        Terminal 2: python 3_daily_analytics.py --account Intranet
        Terminal 3: python 3_daily_analytics.py --account Harper

    After all accounts complete, merge with:
        python merge_analytics_dbs.py
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from tqdm import tqdm

# Add scripts directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    load_secrets,
    setup_logging,
    get_output_paths,
    BrightcoveAuthManager,
    RetryConfig,
    robust_api_call,
    init_analytics_db,
    upsert_daily_analytics,
    get_all_video_max_dates,
    get_db_stats,
    calculate_overlap_start_date,
)

# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_NAME = "3_daily_analytics"


# =============================================================================
# ANALYTICS API CALLS
# =============================================================================

def fetch_daily_summary(
    video_id: str,
    account_id: str,
    from_date: str,
    to_date: str,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    logger
) -> List[Dict]:
    """
    Fetch daily summary metrics for a video.

    Returns list of daily records with metrics.
    """
    url = "https://analytics.api.brightcove.com/v1/data"

    params = {
        "accounts": account_id,
        "dimensions": "date",
        "where": f"video=={video_id}",
        "fields": ",".join([
            "video_view", "video_impression", "play_rate",
            "engagement_score", "video_engagement_1", "video_engagement_25",
            "video_engagement_50", "video_engagement_75", "video_engagement_100",
            "video_percent_viewed", "video_seconds_viewed"
        ]),
        "from": from_date,
        "to": to_date,
        "limit": 366,  # Max one year of daily data
        "sort": "date"
    }

    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}

    response = robust_api_call(
        url=url,
        headers=headers,
        params=params,
        retry_config=retry_config,
        proxies=proxies,
        logger=logger
    )

    if response:
        return response.json().get("items", [])
    return []


def fetch_daily_device_breakdown(
    video_id: str,
    account_id: str,
    from_date: str,
    to_date: str,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    logger
) -> Dict[str, Dict[str, int]]:
    """
    Fetch device breakdown per date for a video.

    Returns dict: {date: {device_type: views}}
    """
    url = "https://analytics.api.brightcove.com/v1/data"

    params = {
        "accounts": account_id,
        "dimensions": "date,device_type",
        "where": f"video=={video_id}",
        "fields": "video_view",
        "from": from_date,
        "to": to_date,
        "limit": 2000,  # Date * device types
        "sort": "date"
    }

    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}

    response = robust_api_call(
        url=url,
        headers=headers,
        params=params,
        retry_config=retry_config,
        proxies=proxies,
        logger=logger
    )

    if not response:
        return {}

    items = response.json().get("items", [])

    # Group by date
    by_date = {}
    for item in items:
        date = item.get("date")
        device = item.get("device_type", "other").lower()
        views = item.get("video_view", 0)

        if date not in by_date:
            by_date[date] = {}
        by_date[date][device] = views

    return by_date


# =============================================================================
# DATA PROCESSING
# =============================================================================

def extract_video_metadata(video: Dict, account_name: str) -> Dict:
    """
    Extract CMS metadata from enriched video object.

    Returns dict with all metadata fields.
    """
    return {
        "channel": account_name,
        "account_id": video.get("account_id", ""),
        "video_id": video.get("id", ""),
        "name": video.get("name", ""),
        "created_at": video.get("created_at", ""),
        "published_at": video.get("published_at", ""),
        "original_filename": video.get("original_filename", ""),
        "created_by": video.get("created_by", ""),
        "video_duration": video.get("duration", 0),
        "tags": ",".join(video.get("tags", [])) if isinstance(video.get("tags"), list) else video.get("tags", ""),
        "reference_id": video.get("reference_id", ""),
        "dt_last_viewed": video.get("dt_last_viewed", ""),
        # Standard custom fields (may be cf_ prefixed or not)
        "video_content_type": video.get("cf_video_content_type", video.get("video_content_type", "")),
        "video_length": video.get("cf_video_length", video.get("video_length", "")),
        "video_category": video.get("cf_video_category", video.get("video_category", "")),
        "country": video.get("cf_country", video.get("country", "")),
        "language": video.get("cf_language", video.get("language", "")),
        "business_unit": video.get("cf_business_unit", video.get("business_unit", "")),
        # Harper additional fields
        "cf_relatedlinkname": video.get("cf_relatedlinkname", ""),
        "cf_relatedlink": video.get("cf_relatedlink", ""),
        "cf_video_owner_email": video.get("cf_video_owner_email", ""),
        "cf_1a_comms_sign_off": video.get("cf_1a_comms_sign_off", ""),
        "cf_1b_comms_sign_off_approver": video.get("cf_1b_comms_sign_off_approver", ""),
        "cf_2a_data_classification_disclaimer": video.get("cf_2a_data_classification_disclaimer", ""),
        "cf_3a_records_management_disclaimer": video.get("cf_3a_records_management_disclaimer", ""),
        "cf_4a_archiving_disclaimer_comms_branding": video.get("cf_4a_archiving_disclaimer_comms_branding", ""),
        "cf_4b_unique_sharepoint_id": video.get("cf_4b_unique_sharepoint_id", ""),
    }


def merge_analytics_with_metadata(
    summary_items: List[Dict],
    device_by_date: Dict[str, Dict[str, int]],
    video_metadata: Dict,
    report_timestamp: str,
    data_type: str
) -> List[Dict]:
    """
    Merge daily analytics with video metadata.

    Returns list of complete row dicts.
    """
    rows = []

    for item in summary_items:
        date = item.get("date")

        # Device breakdown
        devices = device_by_date.get(date, {})

        row = {
            **video_metadata,
            "date": date,
            # Analytics
            "video_view": item.get("video_view", 0),
            "video_impression": item.get("video_impression", 0),
            "play_rate": item.get("play_rate", 0),
            "engagement_score": item.get("engagement_score", 0),
            "video_engagement_1": item.get("video_engagement_1", 0),
            "video_engagement_25": item.get("video_engagement_25", 0),
            "video_engagement_50": item.get("video_engagement_50", 0),
            "video_engagement_75": item.get("video_engagement_75", 0),
            "video_engagement_100": item.get("video_engagement_100", 0),
            "video_percent_viewed": item.get("video_percent_viewed", 0),
            "video_seconds_viewed": item.get("video_seconds_viewed", 0),
            # Device breakdown
            "views_desktop": devices.get("desktop", 0),
            "views_mobile": devices.get("mobile", 0),
            "views_tablet": devices.get("tablet", 0),
            "views_other": devices.get("other", 0) + devices.get("tv", 0) + devices.get("connected_tv", 0),
            # Meta
            "report_generated_on": report_timestamp,
            "data_type": data_type,
        }

        rows.append(row)

    return rows


# =============================================================================
# YEAR PROCESSING
# =============================================================================

def process_year(
    year: int,
    accounts: Dict,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    paths: Dict,
    conn,
    video_max_dates: Dict,
    overlap_days: int,
    logger
) -> int:
    """
    Process a single year for all accounts.

    Uses overlap-based incremental fetching to handle Brightcove lag.

    Returns total rows written.
    """
    report_timestamp = datetime.now().isoformat()
    total_rows = 0

    # Date range for year
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"
    today = datetime.now().strftime("%Y-%m-%d")

    # For current year, don't go past today
    if year == datetime.now().year:
        year_end = min(year_end, today)

    for account_name, account_config in accounts.items():
        account_id = account_config['account_id']

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {account_name} {year}")
        logger.info(f"{'='*60}")

        # Load CMS data
        cms_path = paths['analytics'] / f"{account_name}_cms_enriched.json"
        if not cms_path.exists():
            logger.warning(f"CMS not found: {cms_path}. Skipping.")
            continue

        with open(cms_path, 'r', encoding='utf-8') as f:
            videos = json.load(f)

        logger.info(f"Processing {len(videos)} videos for {year_start} to {year_end}")

        # PRE-LOOP ANALYSIS: Count what will be skipped/processed
        logger.info("Analyzing videos before processing...")
        will_skip_created_after = 0
        will_skip_has_data = 0
        will_process = 0
        found_in_duckdb = 0
        not_in_duckdb = 0

        for video in videos:
            video_id = str(video.get("id"))
            key = (str(account_id), video_id)

            # Check created_at
            created_at = video.get("created_at", "")
            if created_at:
                created_date = created_at[:10] if len(created_at) >= 10 else ""
                if created_date > year_end:
                    will_skip_created_after += 1
                    continue

            # Check if has data in DuckDB
            last_processed = video_max_dates.get(key)
            if last_processed:
                found_in_duckdb += 1
                start_date = calculate_overlap_start_date(last_processed, year_start, overlap_days)
                if start_date > year_end or last_processed >= year_end:
                    will_skip_has_data += 1
                    continue
            else:
                not_in_duckdb += 1

            will_process += 1

        logger.info(f"  Videos created after {year_end}: {will_skip_created_after} (will skip)")
        logger.info(f"  Videos found in DuckDB: {found_in_duckdb}")
        logger.info(f"  Videos NOT in DuckDB: {not_in_duckdb}")
        logger.info(f"  Videos with complete data: {will_skip_has_data} (will skip)")
        logger.info(f"  Videos needing API calls: {will_process}")
        logger.info(f"  Estimated time: ~{will_process * 2.5 / 60:.1f} minutes (assuming 2.5s per API call)")

        if will_process > 500:
            logger.warning(f"  This will make {will_process} API calls - consider if this is expected!")

        # Sanity check
        if found_in_duckdb < will_skip_has_data:
            logger.error(f"  ERROR: Found {found_in_duckdb} in DuckDB but skipping {will_skip_has_data} - logic error!")

        rows_written = 0
        batch_rows = []
        batch_size = 100  # Commit every N videos

        # Skip counters for diagnostics
        skip_created_after = 0
        skip_start_beyond_end = 0
        skip_already_complete = 0
        api_calls_made = 0

        for video in tqdm(videos, desc=f"{account_name} {year}"):
            video_id = str(video.get("id"))  # Convert to string to match DuckDB keys
            key = (str(account_id), video_id)

            # Skip videos created after this year - they can't have historical data
            created_at = video.get("created_at", "")
            if created_at:
                # Extract just the date part (YYYY-MM-DD) from ISO timestamp
                created_date = created_at[:10] if len(created_at) >= 10 else ""
                if created_date > year_end:
                    skip_created_after += 1
                    continue  # Video didn't exist in this year

            # Get last processed date for this video
            last_processed = video_max_dates.get(key)

            # Calculate start date with overlap
            start_date = calculate_overlap_start_date(
                last_processed_date=last_processed,
                year_start=year_start,
                overlap_days=overlap_days
            )

            # Skip if start_date is beyond year_end
            if start_date > year_end:
                skip_start_beyond_end += 1
                continue

            # Only fetch if within this year's range
            if last_processed and last_processed >= year_end:
                skip_already_complete += 1
                continue

            # If we reach here, we're making an API call
            api_calls_made += 1

            try:
                # Fetch analytics
                summary = fetch_daily_summary(
                    video_id=video_id,
                    account_id=account_id,
                    from_date=start_date,
                    to_date=year_end,
                    auth_manager=auth_manager,
                    retry_config=retry_config,
                    proxies=proxies,
                    logger=logger
                )

                if not summary:
                    continue

                device_breakdown = fetch_daily_device_breakdown(
                    video_id=video_id,
                    account_id=account_id,
                    from_date=start_date,
                    to_date=year_end,
                    auth_manager=auth_manager,
                    retry_config=retry_config,
                    proxies=proxies,
                    logger=logger
                )

                # Extract metadata
                metadata = extract_video_metadata(video, account_name)
                metadata["account_id"] = account_id

                # Merge and collect rows
                rows = merge_analytics_with_metadata(
                    summary, device_breakdown, metadata,
                    report_timestamp, f"year_{year}"
                )

                batch_rows.extend(rows)
                rows_written += len(rows)

                # Update max date for this video
                if rows:
                    max_date = max(r["date"] for r in rows)
                    video_max_dates[key] = max_date

                # Batch commit
                if len(batch_rows) >= batch_size * 30:  # ~30 days per video avg
                    upsert_daily_analytics(conn, batch_rows, logger)
                    batch_rows = []

            except Exception as e:
                logger.warning(f"Failed video {video_id}: {e}")
                continue

        # Final batch commit
        if batch_rows:
            upsert_daily_analytics(conn, batch_rows, logger)

        total_rows += rows_written
        logger.info(f"Completed {account_name} {year}: {rows_written} rows")
        logger.info(f"  Skip stats: created_after={skip_created_after}, start>end={skip_start_beyond_end}, already_complete={skip_already_complete}, API_calls={api_calls_made}")

    return total_rows


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch daily analytics per video from Brightcove API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Parallel Processing:
    To run multiple accounts in parallel, use separate terminals:
        Terminal 1: python 3_daily_analytics.py --account Internet
        Terminal 2: python 3_daily_analytics.py --account Intranet
        Terminal 3: python 3_daily_analytics.py --account Harper

    Each account writes to its own DuckDB file, avoiding write conflicts.
    After all accounts complete, run merge_analytics_dbs.py to combine.

Examples:
    # Process all accounts sequentially (single DB)
    python 3_daily_analytics.py

    # Process single account (for parallel execution)
    python 3_daily_analytics.py --account Internet

    # Process specific years only
    python 3_daily_analytics.py --years 2025 2026

    # Show database statistics only (no data collection)
    python 3_daily_analytics.py --stats
        """
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Process only this account (creates account-specific DuckDB file)'
    )
    parser.add_argument(
        '--years',
        type=int,
        nargs='+',
        help='Process only these years (default: all configured years)'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics and exit (no data collection)'
    )
    return parser.parse_args()


def print_db_stats(db_path, logger):
    """Print database statistics and exit."""
    import os

    if not db_path.exists():
        logger.info(f"Database not found: {db_path}")
        return

    conn = init_analytics_db(db_path)
    stats = get_db_stats(conn)

    # Get file size
    file_size_mb = db_path.stat().st_size / (1024 * 1024)

    logger.info("=" * 60)
    logger.info("DATABASE STATISTICS")
    logger.info("=" * 60)
    logger.info(f"File: {db_path}")
    logger.info(f"Size: {file_size_mb:.1f} MB")
    logger.info(f"Total rows: {stats['total_rows']:,}")
    logger.info(f"Unique videos: {stats['unique_videos']:,}")

    if stats['date_range'][0]:
        logger.info(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")

    if stats['rows_by_account']:
        logger.info("\nRows by account:")
        for account_id, count in sorted(stats['rows_by_account'].items()):
            logger.info(f"  {account_id}: {count:,}")

    conn.close()


def main():
    # Parse arguments
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)

    # Handle --stats flag (show stats and exit)
    if args.stats:
        if args.account:
            db_path = paths['output'] / f"analytics_{args.account}.duckdb"
        else:
            db_path = paths['output'] / "analytics.duckdb"
        print_db_stats(db_path, logger)
        return

    logger.info("=" * 60)
    logger.info("Starting daily analytics collection (DuckDB mode)")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    secrets = load_secrets()
    settings = config['settings']

    # Year settings
    analytics_settings = settings['daily_analytics']
    historical_years = analytics_settings.get('historical_years', [2024, 2025])
    current_year = analytics_settings.get('current_year', 2026)
    overlap_days = analytics_settings.get('overlap_days', 7)

    # Combine all years
    all_years = sorted(set(historical_years + [current_year]))

    # Filter years if specified
    if args.years:
        all_years = [y for y in all_years if y in args.years]

    logger.info(f"Years to process: {all_years}")
    logger.info(f"Overlap days for lag compensation: {overlap_days}")

    # Setup authentication
    proxies = secrets.get('proxies') if settings['proxy']['enabled'] else None
    auth_manager = BrightcoveAuthManager(
        client_id=secrets['client_id'],
        client_secret=secrets['client_secret'],
        proxies=proxies
    )

    retry_config = RetryConfig.from_settings(settings)
    all_accounts = config['accounts']['accounts']

    # Filter to single account if --account is specified
    single_account_mode = args.account is not None
    if single_account_mode:
        if args.account not in all_accounts:
            logger.error(f"Account '{args.account}' not found. Available: {list(all_accounts.keys())}")
            return
        accounts = {args.account: all_accounts[args.account]}
        logger.info(f"Single account mode: {args.account}")
    else:
        accounts = all_accounts

    # Determine DuckDB file path
    if single_account_mode:
        db_path = paths['output'] / f"analytics_{args.account}.duckdb"
        logger.info(f"Using account-specific DB: {db_path.name}")
    else:
        db_path = paths['output'] / "analytics.duckdb"
        logger.info(f"Using central DB: {db_path.name}")

    # Initialize DuckDB
    conn = init_analytics_db(db_path)

    # Get existing max dates for incremental processing
    video_max_dates = get_all_video_max_dates(conn)
    logger.info(f"Found {len(video_max_dates)} existing video records")

    # Process each year
    total_rows = 0
    for year in all_years:
        logger.info(f"\n{'#'*60}")
        logger.info(f"YEAR: {year}")
        logger.info(f"{'#'*60}")

        year_rows = process_year(
            year=year,
            accounts=accounts,
            auth_manager=auth_manager,
            retry_config=retry_config,
            proxies=proxies,
            paths=paths,
            conn=conn,
            video_max_dates=video_max_dates,
            overlap_days=overlap_days,
            logger=logger
        )
        total_rows += year_rows

    # Get final stats
    stats = get_db_stats(conn)

    # Close connection
    conn.close()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Daily analytics collection completed")
    logger.info("=" * 60)
    logger.info(f"Rows written this run: {total_rows:,}")
    logger.info(f"Total rows in DB: {stats['total_rows']:,}")
    logger.info(f"Unique videos: {stats['unique_videos']:,}")
    logger.info(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
    logger.info(f"\nOutput: {db_path}")

    if single_account_mode:
        logger.info(f"\nNote: Account-specific DB created for parallel processing.")
        logger.info(f"Run merge_analytics_dbs.py after all accounts complete to combine.")


if __name__ == "__main__":
    main()
