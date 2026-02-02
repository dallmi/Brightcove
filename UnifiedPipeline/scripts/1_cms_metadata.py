"""
1_cms_metadata.py - Fetch CMS metadata for all Brightcove accounts

Purpose:
    Fetches complete video metadata (including custom fields) for all 11 accounts.
    Outputs JSON and CSV files per account.

IMPORTANT: Run this script at EVERY execution to capture newly created videos!
    New videos created since the last run will not appear in analytics
    if this script is skipped.

Runtime: ~10 minutes for all accounts

Run frequency: Every execution (monthly/weekly)

Features:
    - Per-account checkpointing for resume capability
    - All custom fields extracted and flattened
    - Progress tracking with tqdm

Input:
    - secrets.json (credentials)
    - config/accounts.json (account configuration)

Output:
    - output/cms/{account}_cms_metadata.json (full API response)
    - output/cms/{account}_cms_metadata.csv (flattened with custom fields)
"""

import sys
import csv
import json
import argparse
from pathlib import Path
from datetime import datetime
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
    save_checkpoint_atomic,
    load_checkpoint,
)

# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_NAME = "1_cms_metadata"

# CSV fields - standard CMS fields
STANDARD_FIELDS = [
    "account_id", "id", "name", "original_filename", "description",
    "updated_at", "created_at", "published_at", "created_by",
    "ad_keys", "clip_source_video_id", "complete", "cue_points",
    "delivery_type", "digital_master_id", "duration", "economics",
    "folder_id", "geo", "has_digital_master", "images", "link",
    "long_description", "projection", "reference_id", "schedule",
    "sharing", "state", "tags", "text_tracks", "transcripts",
    "updated_by", "playback_rights_id", "ingestion_profile_id"
]

# Known custom fields to extract
KNOWN_CUSTOM_FIELDS = [
    "video_content_type", "relatedlinkname", "relatedlink",
    "country", "language", "business_unit", "video_category",
    "video_length", "video_owner_email", "1a_comms_sign_off",
    "1b_comms_sign_off_approver", "2a_data_classification_disclaimer",
    "3a_records_management_disclaimer",
    "4a_archiving_disclaimer_comms_branding", "4b_unique_sharepoint_id"
]


# =============================================================================
# CMS FETCHING
# =============================================================================

def fetch_video_count(
    account_id: str,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    logger
) -> int:
    """Get total video count for progress tracking."""
    url = f"https://cms.api.brightcove.com/v1/accounts/{account_id}/counts/videos"
    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}

    response = robust_api_call(
        url=url,
        headers=headers,
        retry_config=retry_config,
        proxies=proxies,
        logger=logger
    )

    if response:
        return response.json().get("count", 0)
    return 0


def fetch_all_videos(
    account_id: str,
    auth_manager: BrightcoveAuthManager,
    retry_config: RetryConfig,
    proxies: dict,
    logger,
    checkpoint_path: Path,
    checkpoint_data: dict
) -> list:
    """
    Fetch all videos for an account with checkpointing.

    Resumes from checkpoint if interrupted.
    """
    all_videos = checkpoint_data.get("videos", [])
    offset = checkpoint_data.get("offset", 0)
    limit = 100

    # Get total count for progress bar
    total = fetch_video_count(
        account_id, auth_manager, retry_config, proxies, logger
    )

    logger.info(f"Total videos to fetch: {total}")

    pbar = tqdm(
        total=total,
        initial=len(all_videos),
        unit="videos",
        desc=f"Fetching videos"
    )

    while True:
        token = auth_manager.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        url = f"https://cms.api.brightcove.com/v1/accounts/{account_id}/videos"
        params = {
            "limit": limit,
            "offset": offset,
            "sort": "created_at"
        }

        response = robust_api_call(
            url=url,
            headers=headers,
            params=params,
            retry_config=retry_config,
            proxies=proxies,
            logger=logger
        )

        if not response:
            logger.error(f"Failed to fetch videos at offset {offset}")
            break

        batch = response.json()
        if not batch:
            break

        all_videos.extend(batch)
        offset += len(batch)
        pbar.update(len(batch))

        # Save checkpoint after each batch
        checkpoint_data["videos"] = all_videos
        checkpoint_data["offset"] = offset
        save_checkpoint_atomic(checkpoint_path, checkpoint_data)

        if len(batch) < limit:
            break

    pbar.close()
    return all_videos


# =============================================================================
# OUTPUT GENERATION
# =============================================================================

def discover_custom_fields(videos: list) -> list:
    """Discover all unique custom field keys across all videos."""
    cf_keys = set()
    for video in videos:
        cf = video.get("custom_fields", {}) or {}
        cf_keys.update(cf.keys())
    return sorted(list(cf_keys))


def write_json_output(videos: list, output_path: Path, logger):
    """Write videos to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(videos, f, indent=2)

    logger.info(f"JSON written: {output_path} ({len(videos)} videos)")


def write_csv_output(videos: list, output_path: Path, logger):
    """Write videos to CSV file with flattened custom fields."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Discover all custom fields
    all_cf_keys = discover_custom_fields(videos)

    # Build fieldnames: standard fields + cf_ prefixed custom fields
    cf_fieldnames = [f"cf_{k}" for k in all_cf_keys]
    fieldnames = STANDARD_FIELDS + cf_fieldnames

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for video in videos:
            row = {}

            # Standard fields
            for field in STANDARD_FIELDS:
                value = video.get(field, "")
                # Handle tags as comma-separated string
                if field == "tags" and isinstance(value, list):
                    value = ",".join(value)
                # Handle complex objects as JSON strings
                elif isinstance(value, (dict, list)):
                    value = json.dumps(value)
                row[field] = value

            # Custom fields (with cf_ prefix)
            cf = video.get("custom_fields", {}) or {}
            for cf_key in all_cf_keys:
                row[f"cf_{cf_key}"] = cf.get(cf_key, "")

            writer.writerow(row)

    logger.info(f"CSV written: {output_path} ({len(videos)} videos)")


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch CMS metadata for all Brightcove accounts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script fetches complete video metadata (including custom fields) for all accounts.

IMPORTANT: Run this script at EVERY execution to capture newly created videos!

Output:
    - output/cms/{account}_cms_metadata.json (full API response)
    - output/cms/{account}_cms_metadata.csv (flattened with custom fields)

Examples:
    python 1_cms_metadata.py                    # Process all accounts
    python 1_cms_metadata.py --account Harper   # Process only Harper account
        """
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Process only this specific account (by name from accounts.json)'
    )
    return parser.parse_args()


def main():
    # Parse arguments
    args = parse_args()

    # Setup
    paths = get_output_paths()
    logger = setup_logging(paths['logs'], SCRIPT_NAME)
    logger.info("=" * 60)
    logger.info("Starting CMS metadata collection")
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
    checkpoint_path = paths['checkpoints'] / "cms_checkpoint.json"

    # Load existing checkpoint
    checkpoint = load_checkpoint(checkpoint_path) or {
        "accounts_completed": [],
        "current_account": None,
        "current_data": {}
    }

    accounts_completed = set(checkpoint.get("accounts_completed", []))

    # Process each account
    accounts = config['accounts']['accounts']

    # Filter to single account if specified
    if args.account:
        if args.account not in accounts:
            logger.error(f"Account '{args.account}' not found in accounts.json")
            logger.info(f"Available accounts: {', '.join(accounts.keys())}")
            sys.exit(1)
        accounts = {args.account: accounts[args.account]}
        # Don't skip based on checkpoint when running single account
        accounts_completed = set()
        logger.info(f"Running for single account: {args.account}")

    for account_name, account_config in accounts.items():
        if account_name in accounts_completed:
            logger.info(f"Skipping {account_name} (already completed)")
            continue

        account_id = account_config['account_id']
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing: {account_name} ({account_id})")
        logger.info(f"{'='*60}")

        # Get checkpoint data for this account
        if checkpoint.get("current_account") == account_name:
            account_checkpoint = checkpoint.get("current_data", {})
        else:
            account_checkpoint = {}
            checkpoint["current_account"] = account_name
            checkpoint["current_data"] = account_checkpoint

        try:
            # Fetch all videos
            videos = fetch_all_videos(
                account_id=account_id,
                auth_manager=auth_manager,
                retry_config=retry_config,
                proxies=proxies,
                logger=logger,
                checkpoint_path=checkpoint_path,
                checkpoint_data=account_checkpoint
            )

            # Write outputs
            json_path = paths['cms'] / f"{account_name}_cms_metadata.json"
            csv_path = paths['cms'] / f"{account_name}_cms_metadata.csv"

            write_json_output(videos, json_path, logger)
            write_csv_output(videos, csv_path, logger)

            # Mark account as completed
            accounts_completed.add(account_name)
            checkpoint["accounts_completed"] = list(accounts_completed)
            checkpoint["current_account"] = None
            checkpoint["current_data"] = {}
            save_checkpoint_atomic(checkpoint_path, checkpoint)

            logger.info(f"Completed {account_name}: {len(videos)} videos")

        except Exception as e:
            logger.error(f"Failed to process {account_name}: {e}")
            raise

    # All done - remove checkpoint
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        logger.info("Checkpoint file removed (all accounts completed)")

    logger.info("\n" + "=" * 60)
    logger.info("CMS metadata collection completed successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
