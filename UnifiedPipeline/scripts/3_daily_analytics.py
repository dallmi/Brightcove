"""
3_daily_analytics.py - Fetch detailed daily analytics per video

Purpose:
    Fetches detailed daily analytics (views, engagement, device breakdown)
    with different strategies for historical vs. current year data.

Strategy:
    - Historical years (2024, 2025): Fetch ALL videos, run ONCE (first execution only)
    - Current year (2026): Only videos with views in last 90 days, run incrementally

Runtime:
    - First run (historical + current): ~4-8 hours
    - Subsequent runs (current only): ~30-60 minutes

Run frequency: Every execution, but historical data is auto-skipped after first run

Prerequisites: Scripts 1 and 2 MUST run before this script at every execution!
    - 1_cms_metadata.py captures new videos
    - 2_dt_last_viewed.py updates dt_last_viewed for 90-day filtering

Features:
    - Separate checkpoints for historical vs. current data
    - Auto-detection: skips historical years if already completed
    - JSONL checkpoint for granular resume
    - Device breakdown (desktop, mobile, tablet, other)

Input:
    - output/analytics/{account}_cms_enriched.json (from script 2)
    - secrets.json (credentials)
    - config/settings.json (year configuration)

Output:
    - checkpoints/daily_historical.jsonl (2024 + 2025 data, run once)
    - checkpoints/daily_current.jsonl (2026 data, incremental)
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
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
    load_checkpoint_jsonl,
    append_checkpoint_line,
    save_checkpoint_atomic,
    load_checkpoint,
)

# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_NAME = "3_daily_analytics"

# Output CSV fields (Reporting 32 + Harper additions)
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
    "report_generated_on", "data_type"
]


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

def build_video_max_date_map(checkpoint_rows: List[Dict]) -> Dict[Tuple[str, str], str]:
    """
    Build map of (account_id, video_id) -> max processed date.

    Used to determine resume point.
    """
    max_dates = {}

    for row in checkpoint_rows:
        key = (row.get("account_id"), row.get("video_id"))
        date = row.get("date")

        if key not in max_dates or (date and date > max_dates[key]):
            max_dates[key] = date

    return max_dates


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
# HISTORICAL DATA PROCESSING (2024 + 2025)
# =============================================================================

def process_historical_years(
    accounts: Dict,
    historical_years: List[int],
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    paths: Dict,
    logger
) -> int:
    """
    Process historical years (all videos, no 90-day filter).

    Returns total rows written.
    """
    checkpoint_path = paths['checkpoints'] / "daily_historical.jsonl"
    status_path = paths['checkpoints'] / "historical_status.json"

    # Check if already completed
    status = load_checkpoint(status_path) or {"completed_accounts": {}}

    # Load existing checkpoint
    checkpoint_rows = load_checkpoint_jsonl(checkpoint_path)
    video_max_dates = build_video_max_date_map(checkpoint_rows)

    logger.info(f"Historical checkpoint: {len(checkpoint_rows)} existing rows")

    total_rows = len(checkpoint_rows)
    report_timestamp = datetime.now().isoformat()

    for account_name, account_config in accounts.items():
        account_id = account_config['account_id']

        # Check if this account's historical data is complete
        account_status = status["completed_accounts"].get(account_name, {})
        completed_years = set(account_status.get("years", []))

        for year in historical_years:
            if year in completed_years:
                logger.info(f"Skipping {account_name} {year} (already completed)")
                continue

            logger.info(f"\n{'='*60}")
            logger.info(f"Processing HISTORICAL: {account_name} {year}")
            logger.info(f"{'='*60}")

            # Load CMS data
            cms_path = paths['analytics'] / f"{account_name}_cms_enriched.json"
            if not cms_path.exists():
                logger.warning(f"CMS not found: {cms_path}. Skipping.")
                continue

            with open(cms_path, 'r', encoding='utf-8') as f:
                videos = json.load(f)

            # For historical: ALL videos (no 90-day filter)
            from_date = f"{year}-01-01"
            to_date = f"{year}-12-31"

            logger.info(f"Processing {len(videos)} videos for {from_date} to {to_date}")

            rows_written = 0

            for video in tqdm(videos, desc=f"{account_name} {year}"):
                video_id = video.get("id")
                key = (account_id, video_id)

                # Determine start date (resume point)
                last_processed = video_max_dates.get(key)
                if last_processed and last_processed >= from_date and last_processed < to_date:
                    start_date = (
                        datetime.strptime(last_processed, "%Y-%m-%d") + timedelta(days=1)
                    ).strftime("%Y-%m-%d")
                elif last_processed and last_processed >= to_date:
                    continue  # Already done for this year
                else:
                    start_date = from_date

                if start_date > to_date:
                    continue

                try:
                    # Fetch analytics
                    summary = fetch_daily_summary(
                        video_id=video_id,
                        account_id=account_id,
                        from_date=start_date,
                        to_date=to_date,
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
                        to_date=to_date,
                        auth_manager=auth_manager,
                        retry_config=retry_config,
                        proxies=proxies,
                        logger=logger
                    )

                    # Extract metadata
                    metadata = extract_video_metadata(video, account_name)
                    metadata["account_id"] = account_id

                    # Merge and write rows
                    rows = merge_analytics_with_metadata(
                        summary, device_breakdown, metadata,
                        report_timestamp, f"historical_{year}"
                    )

                    for row in rows:
                        append_checkpoint_line(checkpoint_path, row)
                        rows_written += 1
                        video_max_dates[key] = row["date"]

                except Exception as e:
                    logger.warning(f"Failed video {video_id}: {e}")
                    continue

            total_rows += rows_written
            logger.info(f"Completed {account_name} {year}: {rows_written} rows")

            # Mark year as completed
            if account_name not in status["completed_accounts"]:
                status["completed_accounts"][account_name] = {"years": []}
            status["completed_accounts"][account_name]["years"].append(year)
            save_checkpoint_atomic(status_path, status)

    return total_rows


# =============================================================================
# CURRENT YEAR PROCESSING (2026 - only recent activity)
# =============================================================================

def process_current_year(
    accounts: Dict,
    current_year: int,
    days_back_filter: int,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    paths: Dict,
    logger
) -> int:
    """
    Process current year (only videos with recent activity).

    Returns total rows written.
    """
    checkpoint_path = paths['checkpoints'] / "daily_current.jsonl"

    # Load existing checkpoint
    checkpoint_rows = load_checkpoint_jsonl(checkpoint_path)
    video_max_dates = build_video_max_date_map(checkpoint_rows)

    logger.info(f"Current year checkpoint: {len(checkpoint_rows)} existing rows")

    total_rows = len(checkpoint_rows)
    report_timestamp = datetime.now().isoformat()

    # Date range for current year
    from_date = f"{current_year}-01-01"
    to_date = datetime.now().strftime("%Y-%m-%d")

    # Cutoff for 90-day filter
    cutoff_date = (datetime.now() - timedelta(days=days_back_filter)).strftime("%Y-%m-%d")

    logger.info(f"Current year: {from_date} to {to_date}")
    logger.info(f"90-day filter: only videos with dt_last_viewed >= {cutoff_date}")

    for account_name, account_config in accounts.items():
        account_id = account_config['account_id']

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing CURRENT: {account_name} {current_year}")
        logger.info(f"{'='*60}")

        # Load CMS data
        cms_path = paths['analytics'] / f"{account_name}_cms_enriched.json"
        if not cms_path.exists():
            logger.warning(f"CMS not found: {cms_path}. Skipping.")
            continue

        with open(cms_path, 'r', encoding='utf-8') as f:
            videos = json.load(f)

        # Filter to videos with recent activity (90-day filter)
        active_videos = [
            v for v in videos
            if v.get("dt_last_viewed") and v.get("dt_last_viewed") >= cutoff_date
        ]

        logger.info(f"Filtered to {len(active_videos)} active videos (from {len(videos)} total)")

        rows_written = 0

        for video in tqdm(active_videos, desc=f"{account_name} {current_year}"):
            video_id = video.get("id")
            key = (account_id, video_id)

            # Determine start date (resume point)
            last_processed = video_max_dates.get(key)
            if last_processed and last_processed >= from_date:
                start_date = (
                    datetime.strptime(last_processed, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
            else:
                start_date = from_date

            if start_date > to_date:
                continue

            try:
                # Fetch analytics
                summary = fetch_daily_summary(
                    video_id=video_id,
                    account_id=account_id,
                    from_date=start_date,
                    to_date=to_date,
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
                    to_date=to_date,
                    auth_manager=auth_manager,
                    retry_config=retry_config,
                    proxies=proxies,
                    logger=logger
                )

                # Extract metadata
                metadata = extract_video_metadata(video, account_name)
                metadata["account_id"] = account_id

                # Merge and write rows
                rows = merge_analytics_with_metadata(
                    summary, device_breakdown, metadata,
                    report_timestamp, f"current_{current_year}"
                )

                for row in rows:
                    append_checkpoint_line(checkpoint_path, row)
                    rows_written += 1
                    video_max_dates[key] = row["date"]

            except Exception as e:
                logger.warning(f"Failed video {video_id}: {e}")
                continue

        total_rows += rows_written
        logger.info(f"Completed {account_name}: {rows_written} new rows")

    return total_rows


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting daily analytics collection")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    secrets = load_secrets()
    settings = config['settings']

    # Year settings
    analytics_settings = settings['daily_analytics']
    historical_years = analytics_settings.get('historical_years', [2024, 2025])
    current_year = analytics_settings.get('current_year', 2026)
    days_back_filter = analytics_settings.get('days_back_filter', 90)

    logger.info(f"Historical years: {historical_years}")
    logger.info(f"Current year: {current_year}")
    logger.info(f"90-day filter for current year: {days_back_filter} days")

    # Setup authentication
    proxies = secrets.get('proxies') if settings['proxy']['enabled'] else None
    auth_manager = BrightcoveAuthManager(
        client_id=secrets['client_id'],
        client_secret=secrets['client_secret'],
        proxies=proxies
    )

    retry_config = RetryConfig.from_settings(settings)
    accounts = config['accounts']['accounts']

    # Check if historical data needs processing
    status_path = paths['checkpoints'] / "historical_status.json"
    status = load_checkpoint(status_path) or {"completed_accounts": {}}

    # Check if all historical years are done for all accounts
    all_historical_done = True
    for account_name in accounts.keys():
        account_status = status.get("completed_accounts", {}).get(account_name, {})
        completed_years = set(account_status.get("years", []))
        if not all(y in completed_years for y in historical_years):
            all_historical_done = False
            break

    total_historical = 0
    total_current = 0

    # Process historical years if needed
    if not all_historical_done:
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 1: Processing HISTORICAL data (2024 + 2025)")
        logger.info("=" * 60)

        total_historical = process_historical_years(
            accounts=accounts,
            historical_years=historical_years,
            auth_manager=auth_manager,
            retry_config=retry_config,
            proxies=proxies,
            paths=paths,
            logger=logger
        )
    else:
        logger.info("Historical data already complete - skipping")
        # Count existing historical rows
        hist_path = paths['checkpoints'] / "daily_historical.jsonl"
        if hist_path.exists():
            total_historical = len(load_checkpoint_jsonl(hist_path))

    # Always process current year
    logger.info("\n" + "=" * 60)
    logger.info(f"PHASE 2: Processing CURRENT year ({current_year})")
    logger.info("=" * 60)

    total_current = process_current_year(
        accounts=accounts,
        current_year=current_year,
        days_back_filter=days_back_filter,
        auth_manager=auth_manager,
        retry_config=retry_config,
        proxies=proxies,
        paths=paths,
        logger=logger
    )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Daily analytics collection completed")
    logger.info("=" * 60)
    logger.info(f"Historical data: {total_historical} rows")
    logger.info(f"Current year data: {total_current} rows")
    logger.info(f"Total: {total_historical + total_current} rows")
    logger.info("\nCheckpoint files:")
    logger.info(f"  - checkpoints/daily_historical.jsonl")
    logger.info(f"  - checkpoints/daily_current.jsonl")
    logger.info("\nRun 4_combine_output.py to generate final CSVs")


if __name__ == "__main__":
    main()
