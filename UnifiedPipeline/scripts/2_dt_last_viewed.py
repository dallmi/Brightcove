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
from pathlib import Path
from datetime import datetime
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

    # Use reconciled data for historical windows, live for "now"
    if to_date != "now":
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
) -> bool:
    """
    Process a window, splitting on failure.

    Returns True if successful, False if permanently failed.
    """
    max_depth = 5  # Prevent infinite recursion
    window_key = f"{from_date}_{to_date}"

    if depth > max_depth:
        logger.error(f"Max split depth reached for window {window_key}")
        return False

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
        return True

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
                success = process_window_with_splitting(
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
                    return False

            return True
        else:
            # Cannot split further or is live window
            logger.error(f"Window {window_key} permanently failed: {e}")
            return False


def process_account(
    account_name: str,
    account_id: str,
    window_type: str,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    checkpoint_path: Path,
    checkpoint: dict,
    logger
) -> Dict[str, str]:
    """
    Process all windows for an account with checkpointing.

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
            "last_updated": None
        }

    account_chk = checkpoint["accounts"][account_name]

    if account_chk["status"] == "completed":
        logger.info(f"Account {account_name} already completed, loading results")
        return account_chk["last_map"]

    # Get date bounds
    first_date, last_date = get_date_bounds(
        auth_manager, account_id, retry_config, proxies, logger
    )

    if not first_date:
        logger.warning(f"No analytics data found for {account_name}")
        account_chk["status"] = "completed"
        save_checkpoint_atomic(checkpoint_path, checkpoint)
        return {}

    # Generate windows
    windows = generate_windows(first_date, last_date, window_type)
    logger.info(f"Generated {len(windows)} {window_type} windows")

    # Get completed windows
    completed = set(account_chk["windows_completed"])
    last_map = account_chk["last_map"]

    # Process windows
    account_chk["status"] = "in_progress"
    pending_windows = [w for w in windows if f"{w[0]}_{w[1]}" not in completed]

    for from_date, to_date in tqdm(pending_windows, desc=f"Windows for {account_name}"):
        window_key = f"{from_date}_{to_date}"

        if window_key in completed:
            continue

        logger.info(f"Processing window: {from_date} to {to_date}")

        success = process_window_with_splitting(
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
            account_chk["windows_completed"] = list(completed)
        else:
            account_chk["windows_failed"].append(window_key)

        account_chk["last_map"] = last_map
        account_chk["last_updated"] = datetime.now().isoformat()
        save_checkpoint_atomic(checkpoint_path, checkpoint)

    # Mark as completed
    account_chk["status"] = "completed"
    save_checkpoint_atomic(checkpoint_path, checkpoint)

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


def write_stakeholder_excel(
    videos: List[Dict],
    account_name: str,
    output_dir: Path,
    logger
) -> None:
    """
    Write enriched CMS data to Excel file for stakeholder.

    Creates {account_name}_cms.xlsx in the stakeholder output folder.
    Format matches Harper's channel_cms.xlsx output.
    """
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas not installed. Skipping Excel export. Install with: pip install pandas openpyxl")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    excel_path = output_dir / f"{account_name}_cms.xlsx"

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

def main():
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
                logger=logger
            )

            # Write outputs
            lv_path = paths['analytics'] / f"{account_name}_dt_last_viewed.json"
            enriched_path = paths['analytics'] / f"{account_name}_cms_enriched.json"

            write_last_viewed_json(last_map, lv_path, logger)
            enriched_videos = enrich_cms_metadata(cms_path, last_map, enriched_path, logger)

            # Write Excel for stakeholder (Harper-compatible format)
            write_stakeholder_excel(enriched_videos, account_name, paths['stakeholder'], logger)

            logger.info(f"Completed {account_name}: {len(last_map)} videos with views")

        except Exception as e:
            logger.error(f"Failed to process {account_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("dt_last_viewed calculation completed")

    # Report any failures
    for account_name, account_chk in checkpoint.get("accounts", {}).items():
        failed = account_chk.get("windows_failed", [])
        if failed:
            logger.warning(f"{account_name}: {len(failed)} windows failed")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
