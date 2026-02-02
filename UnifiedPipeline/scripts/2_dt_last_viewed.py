"""
2_dt_last_viewed.py - Calculate last viewed date for all videos

Purpose:
    Calculates dt_last_viewed (all-time last view date) for all videos
    using the Brightcove Analytics API.

IMPORTANT: Run this script at EVERY execution to update dt_last_viewed for all videos!
    - New videos need their dt_last_viewed calculated
    - Existing videos need their dt_last_viewed updated
    - The 90-day filter in script 3 depends on accurate dt_last_viewed

Runtime: ~60-90 minutes for all accounts

Run frequency: Every execution (monthly/weekly)

Features:
    - Adaptive window strategy (monthly for large accounts, quarterly for others)
    - Progressive window splitting on failures (quarterly -> monthly -> weekly -> daily)
    - Per-account + per-window checkpointing
    - 5 retries with exponential backoff and jitter
    - Automatic resume from any failure point

Input:
    - output/cms/{account}_cms_metadata.json (from script 1)
    - secrets.json (credentials)

Output:
    - output/analytics/{account}_dt_last_viewed.json ({video_id: date} mapping)
    - output/analytics/{account}_cms_enriched.json (CMS + dt_last_viewed)
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm
import time

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
    save_checkpoint_atomic,
    load_checkpoint,
    generate_windows,
    split_window,
    get_date_range_days,
)

# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_NAME = "2_dt_last_viewed"


# =============================================================================
# DATE BOUNDS
# =============================================================================

def get_date_bounds(
    auth_manager: BrightcoveAuthManager,
    account_id: str,
    retry_config: RetryConfig,
    proxies: dict,
    logger
) -> Tuple[str, str]:
    """
    Get the earliest and latest dates with any views for an account.

    Returns:
        Tuple of (first_date, last_date) in YYYY-MM-DD format
    """
    url = "https://analytics.api.brightcove.com/v1/data"

    # Get earliest date
    params_asc = {
        "accounts": account_id,
        "dimensions": "date",
        "fields": "video_view",
        "from": "alltime",
        "to": "now",
        "limit": 1,
        "sort": "date"
    }

    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}

    response = robust_api_call(
        url=url,
        headers=headers,
        params=params_asc,
        retry_config=retry_config,
        proxies=proxies,
        logger=logger
    )

    if not response:
        raise RuntimeError(f"Failed to get date bounds for account {account_id}")

    items = response.json().get("items", [])
    if not items:
        return None, None

    first_date = items[0]["date"]

    # Get latest date
    params_desc = {**params_asc, "sort": "-date"}
    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}

    response = robust_api_call(
        url=url,
        headers=headers,
        params=params_desc,
        retry_config=retry_config,
        proxies=proxies,
        logger=logger
    )

    if not response:
        raise RuntimeError(f"Failed to get date bounds for account {account_id}")

    last_date = response.json()["items"][0]["date"]

    logger.info(f"Date bounds: {first_date} to {last_date}")
    return first_date, last_date


# =============================================================================
# ANALYTICS FETCHING
# =============================================================================

def fetch_analytics_slice(
    auth_manager: BrightcoveAuthManager,
    account_id: str,
    from_date: str,
    to_date: str,
    retry_config: RetryConfig,
    proxies: dict,
    logger
) -> List[Dict]:
    """
    Fetch analytics for a time window with pagination.

    Returns list of {video, date, video_view} items.
    """
    url = "https://analytics.api.brightcove.com/v1/data"
    limit = 10000
    offset = 0
    all_items = []

    params = {
        "accounts": account_id,
        "dimensions": "video,date",
        "fields": "video_view",
        "sort": "-date",
        "from": from_date,
        "to": to_date,
        "limit": limit,
        "offset": offset
    }

    # Use reconciled data for historical windows, live data for recent windows
    # Reconciled data takes 24-72h to become available, so use live data for last 3 days
    if to_date != "now":
        try:
            to_dt = datetime.strptime(to_date, "%Y-%m-%d")
            days_ago = (datetime.now() - to_dt).days
            if days_ago >= 3:
                params["reconciled"] = "true"
            # else: use live data (no reconciled param)
        except ValueError:
            # If date parsing fails, default to reconciled
            params["reconciled"] = "true"

    while True:
        headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}
        params["offset"] = offset

        response = robust_api_call(
            url=url,
            headers=headers,
            params=params,
            retry_config=retry_config,
            proxies=proxies,
            logger=logger
        )

        if not response:
            raise RuntimeError(
                f"Failed to fetch analytics for window {from_date} to {to_date}"
            )

        items = response.json().get("items", [])
        if not items:
            break

        all_items.extend(items)
        offset += len(items)

        if len(items) < limit:
            break

    return all_items


def merge_last_views(last_map: Dict[str, str], items: List[Dict]) -> None:
    """
    Update last_map with max date per video from items.

    Modifies last_map in place.
    """
    for item in items:
        video_id = item.get("video")
        date = item.get("date")
        views = item.get("video_view", 0)

        if views > 0 and video_id:
            if video_id not in last_map or date > last_map[video_id]:
                last_map[video_id] = date


# =============================================================================
# WINDOW PROCESSING WITH ADAPTIVE SPLITTING
# =============================================================================

def process_window_with_splitting(
    auth_manager: BrightcoveAuthManager,
    account_id: str,
    from_date: str,
    to_date: str,
    last_map: Dict[str, str],
    retry_config: RetryConfig,
    proxies: dict,
    logger,
    depth: int = 0
) -> Tuple[bool, Optional[str]]:
    """
    Process a window, splitting on failure.

    Returns:
        Tuple of (success, error_message). error_message is None on success.
    """
    max_depth = 5  # Prevent infinite recursion
    window_key = f"{from_date}_{to_date}"

    if depth > max_depth:
        error_msg = f"Max split depth reached for window {window_key}"
        logger.error(error_msg)
        return False, error_msg

    try:
        items = fetch_analytics_slice(
            auth_manager=auth_manager,
            account_id=account_id,
            from_date=from_date,
            to_date=to_date,
            retry_config=retry_config,
            proxies=proxies,
            logger=logger
        )

        merge_last_views(last_map, items)
        return True, None

    except Exception as e:
        days = get_date_range_days(from_date, to_date if to_date != "now" else datetime.now().strftime("%Y-%m-%d"))

        if days > 1 and to_date != "now":
            # Split and retry
            logger.warning(
                f"Window {window_key} failed ({e}). "
                f"Splitting ({days} days -> 2 sub-windows)"
            )

            sub_windows = split_window(from_date, to_date)

            for sub_from, sub_to in sub_windows:
                success, error = process_window_with_splitting(
                    auth_manager=auth_manager,
                    account_id=account_id,
                    from_date=sub_from,
                    to_date=sub_to,
                    last_map=last_map,
                    retry_config=retry_config,
                    proxies=proxies,
                    logger=logger,
                    depth=depth + 1
                )
                if not success:
                    return False, error

            return True, None
        else:
            # Cannot split further or is live window
            error_msg = str(e)
            logger.error(f"Window {window_key} permanently failed: {error_msg}")
            return False, error_msg


def process_account(
    account_name: str,
    account_id: str,
    window_type: str,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    checkpoint_path: Path,
    checkpoint: dict,
    logger,
    overlap_days: int = 3,
    retry_failed: bool = False
) -> Dict[str, str]:
    """
    Process all windows for an account with checkpointing.

    Supports incremental mode: if account was previously completed,
    only fetches data from (last_run_date - overlap_days) to now.

    Args:
        retry_failed: If True, only retry previously failed windows.

    Returns:
        Dict mapping video_id -> dt_last_viewed
    """
    # Get or initialize account checkpoint
    if account_name not in checkpoint["accounts"]:
        checkpoint["accounts"][account_name] = {
            "status": "pending",
            "windows_completed": [],
            "windows_failed": [],
            "last_map": {},
            "last_updated": None,
            "last_run_date": None
        }

    account_chk = checkpoint["accounts"][account_name]

    # Ensure last_run_date field exists (for backwards compatibility)
    # If missing but last_updated exists, derive from it
    if "last_run_date" not in account_chk or account_chk["last_run_date"] is None:
        if account_chk.get("last_updated"):
            # Extract date from ISO timestamp (e.g., "2026-01-12T14:30:00" -> "2026-01-12")
            account_chk["last_run_date"] = account_chk["last_updated"][:10]
            logger.info(f"Migrated last_run_date from last_updated: {account_chk['last_run_date']}")
        else:
            account_chk["last_run_date"] = None

    # Check if this is an incremental update (previously completed)
    is_incremental = (
        account_chk["status"] == "completed" and
        account_chk.get("last_run_date") is not None
    )

    # Get date bounds from API
    first_date, last_date = get_date_bounds(
        auth_manager, account_id, retry_config, proxies, logger
    )

    if not first_date:
        logger.warning(f"No analytics data found for {account_name}")
        account_chk["status"] = "completed"
        save_checkpoint_atomic(checkpoint_path, checkpoint)
        return {}

    # Load existing last_map
    last_map = account_chk["last_map"]

    # RETRY FAILED MODE: Only process previously failed windows
    if retry_failed:
        failed_windows = account_chk.get("windows_failed", [])
        if not failed_windows:
            logger.info(f"No failed windows to retry for {account_name}")
            return last_map

        # Parse failed windows (handle both old string format and new dict format)
        pending_windows = []
        for item in failed_windows:
            if isinstance(item, dict):
                window_key = item.get("window")
            else:
                window_key = item
            # Parse "YYYY-MM-DD_YYYY-MM-DD" format
            parts = window_key.split("_")
            if len(parts) == 2:
                pending_windows.append((parts[0], parts[1]))

        logger.info(f"RETRY FAILED MODE: {account_name}")
        logger.info(f"  Retrying {len(pending_windows)} failed windows")
        completed = set(account_chk.get("windows_completed", []))
        is_incremental = False  # Treat as non-incremental for tracking

    elif is_incremental:
        # INCREMENTAL MODE: Only fetch from (last_run_date - overlap_days) to now
        last_run = account_chk["last_run_date"]

        # Calculate incremental start date with overlap buffer
        last_run_dt = datetime.strptime(last_run, "%Y-%m-%d")
        incremental_start_dt = last_run_dt - timedelta(days=overlap_days)
        incremental_start = incremental_start_dt.strftime("%Y-%m-%d")

        # Don't go before first_date
        if incremental_start < first_date:
            incremental_start = first_date

        logger.info(f"INCREMENTAL MODE: {account_name}")
        logger.info(f"  Last run: {last_run}")
        logger.info(f"  Fetching: {incremental_start} to {last_date} (with {overlap_days} day overlap)")

        # Check if there's anything new to fetch
        if incremental_start > last_date:
            logger.info(f"  No new data since last run")
            return last_map

        # Generate windows only for incremental period
        windows = generate_windows(incremental_start, last_date, window_type)
        logger.info(f"  Generated {len(windows)} {window_type} windows for incremental update")

        # For incremental, we process all windows (don't skip based on windows_completed)
        # because the date range is already limited
        pending_windows = windows
        completed = set()
    else:
        # FULL MODE: First run or resume from interruption
        logger.info(f"FULL MODE: {account_name} (first run or resuming)")

        # Generate windows for full date range
        windows = generate_windows(first_date, last_date, window_type)
        logger.info(f"Generated {len(windows)} {window_type} windows")

        # Get completed windows (for resume capability)
        completed = set(account_chk["windows_completed"])
        pending_windows = [w for w in windows if f"{w[0]}_{w[1]}" not in completed]

    # Process windows
    account_chk["status"] = "in_progress"

    for from_date, to_date in tqdm(pending_windows, desc=f"Windows for {account_name}"):
        window_key = f"{from_date}_{to_date}"

        if window_key in completed:
            continue

        success, error_msg = process_window_with_splitting(
            auth_manager=auth_manager,
            account_id=account_id,
            from_date=from_date,
            to_date=to_date,
            last_map=last_map,
            retry_config=retry_config,
            proxies=proxies,
            logger=logger
        )

        if success:
            completed.add(window_key)
            if not is_incremental:
                # Only track windows_completed for full mode (resume capability)
                account_chk["windows_completed"] = list(completed)
            # Remove from windows_failed if this was a retry
            if retry_failed:
                account_chk["windows_failed"] = [
                    f for f in account_chk["windows_failed"]
                    if (f.get("window") if isinstance(f, dict) else f) != window_key
                ]
        else:
            # Store failure with error message (avoid duplicates)
            existing_keys = [
                f.get("window") if isinstance(f, dict) else f
                for f in account_chk["windows_failed"]
            ]
            if window_key not in existing_keys:
                account_chk["windows_failed"].append({
                    "window": window_key,
                    "error": error_msg,
                    "timestamp": datetime.now().isoformat()
                })
            logger.warning(f"Window failed: {from_date} to {to_date} - {error_msg}")

        account_chk["last_map"] = last_map
        account_chk["last_updated"] = datetime.now().isoformat()
        save_checkpoint_atomic(checkpoint_path, checkpoint)

    # Mark as completed and record last_run_date
    account_chk["status"] = "completed"
    account_chk["last_run_date"] = last_date  # The latest date we fetched up to
    save_checkpoint_atomic(checkpoint_path, checkpoint)

    if is_incremental:
        logger.info(f"Incremental update completed. last_run_date updated to {last_date}")

    return last_map


# =============================================================================
# OUTPUT GENERATION
# =============================================================================

def write_last_viewed_json(
    last_map: Dict[str, str],
    output_path: Path,
    logger
) -> None:
    """Write video_id -> dt_last_viewed mapping to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(last_map, f, indent=2)

    logger.info(f"Last viewed data written: {output_path} ({len(last_map)} videos)")


def enrich_cms_metadata(
    cms_path: Path,
    last_map: Dict[str, str],
    output_path: Path,
    logger
) -> List[Dict]:
    """
    Add dt_last_viewed to CMS metadata and unnest custom_fields.

    This creates the enriched output similar to Harper's channel_cms.json

    Returns the enriched video list for further processing (Excel export).
    """
    with open(cms_path, 'r', encoding='utf-8') as f:
        videos = json.load(f)

    for video in videos:
        video_id = video.get("id")
        video["dt_last_viewed"] = last_map.get(video_id)

        # Unnest custom_fields as cf_* fields
        cf = video.pop("custom_fields", {}) or {}
        for key, value in cf.items():
            video[f"cf_{key}"] = value

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(videos, f, indent=2)

    logger.info(f"Enriched CMS written: {output_path} ({len(videos)} videos)")

    return videos


def write_lifecycle_excel(
    videos: List[Dict],
    account_name: str,
    output_dir: Path,
    logger
) -> None:
    """
    Write enriched CMS data to Excel file for lifecycle management.

    Creates {account_name}_cms.xlsx in the life_cycle_mgmt/{YYYY-MM}/ folder,
    where YYYY-MM is the current year-month (e.g., 2026-01).
    Format matches Harper's channel_cms.xlsx output.
    """
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas not installed. Skipping Excel export. Install with: pip install pandas openpyxl")
        return

    # Create year-month subfolder (e.g., 2026-01, 2026-02, ...)
    year_month = datetime.now().strftime("%Y-%m")
    year_month_dir = output_dir / year_month
    year_month_dir.mkdir(parents=True, exist_ok=True)
    excel_path = year_month_dir / f"{account_name}_cms.xlsx"

    # Define column order (matching Harper format)
    # Fixed fields first, then cf_* fields
    fixed_columns = [
        'account_id', 'id', 'name', 'original_filename', 'description',
        'dt_last_viewed', 'updated_at', 'created_at', 'published_at',
        'created_by', 'duration', 'state', 'reference_id', 'tags',
        'ad_keys', 'clip_source_video_id', 'complete', 'cue_points',
        'delivery_type', 'digital_master_id', 'economics', 'folder_id',
        'geo', 'has_digital_master', 'images', 'link', 'long_description',
        'projection', 'schedule', 'sharing', 'text_tracks', 'transcripts',
        'updated_by', 'playback_rights_id', 'ingestion_profile_id'
    ]

    # Discover all cf_* fields
    cf_columns = set()
    for video in videos:
        for key in video.keys():
            if key.startswith('cf_'):
                cf_columns.add(key)
    cf_columns = sorted(list(cf_columns))

    # Final column order
    all_columns = fixed_columns + cf_columns

    # Create DataFrame
    df = pd.DataFrame(videos)

    # Handle tags - convert list to comma-separated string
    if 'tags' in df.columns:
        df['tags'] = df['tags'].apply(
            lambda x: ','.join(x) if isinstance(x, list) else (x if x else '')
        )

    # Handle complex objects - convert to JSON strings
    for col in ['images', 'geo', 'schedule', 'sharing', 'cue_points', 'text_tracks', 'transcripts', 'link']:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else (x if x else '')
            )

    # Reorder columns (only include columns that exist)
    existing_columns = [c for c in all_columns if c in df.columns]
    # Add any columns not in our predefined list
    extra_columns = [c for c in df.columns if c not in existing_columns]
    final_columns = existing_columns + extra_columns

    df = df[final_columns]

    # Write to Excel
    try:
        df.to_excel(excel_path, index=False, engine='openpyxl')
        logger.info(f"Stakeholder Excel written: {excel_path} ({len(videos)} videos)")
    except Exception as e:
        logger.warning(f"Failed to write Excel (openpyxl may not be installed): {e}")
        logger.warning("Install with: pip install openpyxl")


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Calculate last viewed date for all videos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script calculates dt_last_viewed (all-time last view date) for all videos.

IMPORTANT: Run this script at EVERY execution to update dt_last_viewed!
    - New videos need their dt_last_viewed calculated
    - Existing videos need their dt_last_viewed updated
    - The 90-day filter in script 3 depends on accurate dt_last_viewed

Features:
    - Adaptive window strategy (monthly for large accounts, quarterly for others)
    - Progressive window splitting on failures
    - Per-account + per-window checkpointing

Output:
    - output/analytics/{account}_dt_last_viewed.json
    - output/analytics/{account}_cms_enriched.json

Examples:
    python 2_dt_last_viewed.py                    # Process all accounts
    python 2_dt_last_viewed.py --account impact   # Process only impact account
    python 2_dt_last_viewed.py --account impact --retry-failed  # Retry failed windows only
        """
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Process only this specific account (by name from accounts.json)'
    )
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Only retry previously failed windows (requires --account)'
    )
    return parser.parse_args()


def main():
    # Parse arguments
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting dt_last_viewed calculation")
    logger.info("=" * 60)

    # Load configuration
    config = load_config()
    secrets = load_secrets()
    settings = config['settings']

    # Setup authentication
    proxies = secrets.get('proxies') if settings['proxy']['enabled'] else None
    auth_manager = BrightcoveAuthManager(
        client_id=secrets['client_id'],
        client_secret=secrets['client_secret'],
        proxies=proxies
    )

    retry_config = RetryConfig.from_settings(settings)

    # Checkpoint path
    checkpoint_path = paths['checkpoints'] / "analytics_checkpoint.json"

    # Load existing checkpoint
    checkpoint = load_checkpoint(checkpoint_path) or {"accounts": {}}

    # Process each account
    accounts = config['accounts']['accounts']
    problematic_accounts = settings['windows'].get('problematic_accounts', [])
    overlap_days = settings['windows'].get('incremental_overlap_days', 3)

    # Validate --retry-failed requires --account
    if getattr(args, 'retry_failed', False) and not args.account:
        logger.error("--retry-failed requires --account to be specified")
        sys.exit(1)

    # Filter to single account if specified
    if args.account:
        if args.account not in accounts:
            logger.error(f"Account '{args.account}' not found in accounts.json")
            logger.info(f"Available accounts: {', '.join(accounts.keys())}")
            sys.exit(1)
        accounts = {args.account: accounts[args.account]}
        logger.info(f"Running for single account: {args.account}")

    for account_name, account_config in accounts.items():
        account_id = account_config['account_id']

        # Determine window type (monthly for problematic accounts)
        if account_name in problematic_accounts:
            window_type = "monthly"
        else:
            window_type = account_config.get('initial_window_size', 'quarterly')

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {account_name} ({account_id})")
        logger.info(f"Window type: {window_type}")
        logger.info(f"{'='*60}")

        # Check for CMS metadata
        cms_path = paths['cms'] / f"{account_name}_cms_metadata.json"
        if not cms_path.exists():
            logger.error(
                f"CMS metadata not found: {cms_path}. "
                f"Run 1_cms_metadata.py first."
            )
            continue

        try:
            # Process account
            last_map = process_account(
                account_name=account_name,
                account_id=account_id,
                window_type=window_type,
                auth_manager=auth_manager,
                retry_config=retry_config,
                proxies=proxies,
                checkpoint_path=checkpoint_path,
                checkpoint=checkpoint,
                logger=logger,
                overlap_days=overlap_days,
                retry_failed=getattr(args, 'retry_failed', False)
            )

            # Write outputs
            lv_path = paths['analytics'] / f"{account_name}_dt_last_viewed.json"
            enriched_path = paths['analytics'] / f"{account_name}_cms_enriched.json"

            write_last_viewed_json(last_map, lv_path, logger)
            enriched_videos = enrich_cms_metadata(cms_path, last_map, enriched_path, logger)

            # Write Excel for lifecycle management (Harper-compatible format)
            write_lifecycle_excel(enriched_videos, account_name, paths['life_cycle_mgmt'], logger)

            logger.info(f"Completed {account_name}: {len(last_map)} videos with views")

        except Exception as e:
            logger.error(f"Failed to process {account_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("dt_last_viewed calculation completed")

    # Report failures only for accounts processed in this run
    for account_name in accounts.keys():
        account_chk = checkpoint.get("accounts", {}).get(account_name, {})
        failed = account_chk.get("windows_failed", [])
        if failed:
            logger.warning(f"{account_name}: {len(failed)} windows failed:")
            for item in failed:
                if isinstance(item, dict):
                    window = item.get("window", "unknown")
                    error = item.get("error", "no error details")
                    logger.warning(f"  - {window}: {error}")
                else:
                    # Old format (string only)
                    logger.warning(f"  - {item}")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
