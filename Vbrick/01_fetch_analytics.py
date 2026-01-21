"""
01_fetch_analytics.py - Vbrick Video Analytics Fetcher

This script authenticates with the Vbrick API, retrieves video metadata and daily view statistics
for all active videos uploaded in the past two years, and exports the results to DuckDB and CSV.

Features:
- Incremental updates with 7-day overlap (only fetches new data after first run)
- DuckDB storage for persistent checkpointing
- CSV output for backward compatibility
- Progress tracking with tqdm

Usage:
    python 01_fetch_analytics.py           # Normal run (DuckDB + CSV)
    python 01_fetch_analytics.py --stats   # Show database statistics
    python 01_fetch_analytics.py --no-csv  # Skip CSV output
    python 01_fetch_analytics.py --full    # Ignore checkpoint, fetch all data
"""

import argparse
import csv
import json
import logging
import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone

from tqdm import tqdm

from shared_vbrick import (
    VbrickAuthManager,
    safe_get,
    load_vbrick_config,
    init_vbrick_db,
    upsert_video_daily,
    get_all_video_max_dates,
    calculate_overlap_start_date,
    print_db_stats,
    get_output_dir,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def fetch_all_active_videos(auth_manager, proxies=None, count=100):
    """Fetch all active videos from the past 2 years."""
    videos = []
    scroll_id = None

    # Calculate the date 730 days ago in UTC ISO 8601 format
    two_year_ago = (datetime.now(timezone.utc) - timedelta(days=730)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    logger.debug(f"Fetching videos from date: {two_year_ago}")

    # First request to get total count
    url = f"{auth_manager.base_url}/api/v2/videos/search"
    headers = {"Authorization": f"Bearer {auth_manager.get_token()}"}
    params = {
        "count": count,
        "status": "Active",
        "fromUploadDate": two_year_ago
    }

    data = safe_get(url, headers=headers, params=params, proxies=proxies, logger=logger)
    if not data:
        logger.error("Initial request failed, cannot fetch videos.")
        return []

    total = data.get("totalVideos", 0)
    pbar = tqdm(total=total, desc="Fetching Active Videos", unit="video", dynamic_ncols=True)

    items = data.get("videos", [])
    videos.extend(items)
    pbar.update(len(items))
    scroll_id = data.get("scrollId")

    while scroll_id:
        params["scrollId"] = scroll_id
        data = safe_get(url, headers=headers, params=params, proxies=proxies, logger=logger)
        if not data:
            break

        items = data.get("videos", [])
        if not items:
            break

        videos.extend(items)
        pbar.update(len(items))
        scroll_id = data.get("scrollId")

    pbar.close()
    logger.info(f"Fetched {len(videos)} videos total")
    return videos


def get_video_summary(video_id, auth_manager, start_date=None, end_date=None, proxies=None):
    """Get daily summary statistics for a video."""
    url = f"{auth_manager.base_url}/api/v2/videos/{video_id}/summary-statistics"
    headers = {
        "Authorization": f"Bearer {auth_manager.get_token()}",
        "Accept": "application/json"
    }
    params = {}
    if start_date:
        params["after"] = start_date
    if end_date:
        params["before"] = end_date

    data = safe_get(url, headers=headers, params=params, proxies=proxies, logger=logger)
    return data if data else {}


def group_device_type(device_key):
    """Map device types to standard categories."""
    if device_key == 'PC':
        return 'Desktop'
    elif device_key == "Mobile Device":
        return "Mobile"
    else:
        return "Other"


def group_browser_type(browser_key):
    """Map browser types to standard categories."""
    if browser_key in ['Chrome', "Chrome Mobile"]:
        return "Chrome"
    elif browser_key in ['Microsoft Edge', 'Microsoft Edge mobile']:
        return "Edge"
    else:
        return "Other"


def process_video_analytics(video, summary, report_date):
    """
    Process video analytics into rows for DuckDB.

    Args:
        video: Video metadata dict
        summary: Daily summary statistics dict
        report_date: Date string for report_generated_on field

    Returns:
        List of row dicts ready for upsert
    """
    rows = []

    # Base metadata (same for all daily rows)
    base_row = {
        'video_id': video.get("id"),
        'title': video.get("title"),
        'playback_url': video.get("playbackUrl"),
        'duration': video.get("duration"),
        'when_uploaded': video.get("whenUploaded"),
        'last_viewed': video.get("lastViewed"),
        'when_published': video.get("whenPublished"),
        'uploaded_by': video.get("uploadedBy"),
        'tags': ", ".join(video.get("tags", [])) if isinstance(video.get("tags"), list) else video.get("tags", ""),
        'comment_count': video.get("commentCount"),
        'score': video.get("score"),
        'report_generated_on': report_date,
    }

    # Group device counts
    device_grouped = {'Desktop': 0, 'Mobile': 0, 'Other': 0}
    for d in summary.get('deviceCounts', []):
        group = group_device_type(d.get('key'))
        device_grouped[group] = device_grouped.get(group, 0) + d.get('value', 0)

    # Group browser counts
    browser_grouped = {'Chrome': 0, 'Edge': 0, 'Other': 0}
    for b in summary.get('browserCounts', []):
        group = group_browser_type(b.get('key'))
        browser_grouped[group] = browser_grouped.get(group, 0) + b.get('value', 0)

    # Create a row for each day
    for day in summary.get('totalViewsByDay', []):
        row = base_row.copy()
        row['date'] = day.get('key')
        row['views'] = day.get('value', 0)

        # Device breakdown
        row['device_desktop'] = device_grouped.get('Desktop', 0)
        row['device_mobile'] = device_grouped.get('Mobile', 0)
        row['device_other'] = device_grouped.get('Other', 0)

        # Browser breakdown
        row['browser_chrome'] = browser_grouped.get('Chrome', 0)
        row['browser_edge'] = browser_grouped.get('Edge', 0)
        row['browser_other'] = browser_grouped.get('Other', 0)

        rows.append(row)

    return rows


def convert_rows_for_csv(rows):
    """
    Convert DuckDB rows to CSV format (for backward compatibility).

    Maps column names to match original CSV output.
    """
    csv_rows = []
    for row in rows:
        csv_row = {
            'video_id': row.get('video_id'),
            'title': row.get('title'),
            'playbackUrl': row.get('playback_url'),
            'duration': row.get('duration'),
            'whenUploaded': row.get('when_uploaded'),
            'lastViewed': row.get('last_viewed'),
            'whenPublished': row.get('when_published'),
            'commentCount': row.get('comment_count'),
            'score': row.get('score'),
            'uploadedBy': row.get('uploaded_by'),
            'tags': row.get('tags'),
            'date': row.get('date'),
            'views': row.get('views'),
            'Desktop': row.get('device_desktop'),
            'Mobile': row.get('device_mobile'),
            'Other Device': row.get('device_other'),
            'Chrome': row.get('browser_chrome'),
            'Microsoft Edge': row.get('browser_edge'),
            'Other Browser': row.get('browser_other'),
        }
        csv_rows.append(csv_row)
    return csv_rows


def main():
    parser = argparse.ArgumentParser(description='Fetch Vbrick video analytics')
    parser.add_argument('--stats', action='store_true', help='Show database statistics and exit')
    parser.add_argument('--no-csv', action='store_true', help='Skip CSV output')
    parser.add_argument('--full', action='store_true', help='Ignore checkpoint, fetch all data')
    parser.add_argument('--overlap-days', type=int, default=7, help='Days to overlap for incremental updates')
    args = parser.parse_args()

    # Show stats and exit if requested
    if args.stats:
        print_db_stats(logger=logger)
        return

    # Load configuration
    try:
        cfg = load_vbrick_config()
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    base_url = cfg.get("base_url")
    api_key = cfg.get("api_key")
    api_secret = cfg.get("api_secret")
    proxy_url = cfg.get("proxies")
    overlap_days = cfg.get("duckdb", {}).get("overlap_days", args.overlap_days)

    if not all([base_url, api_key, api_secret]):
        logger.error("base_url, api_key, api_secret required in secrets.json")
        sys.exit(1)

    proxies = proxy_url if proxy_url else None
    auth_mgr = VbrickAuthManager(base_url, api_key, api_secret, proxies, logger=logger)

    # Initialize DuckDB
    conn = init_vbrick_db()
    logger.info("Initialized DuckDB database")

    # Get existing data for incremental updates
    video_max_dates = {} if args.full else get_all_video_max_dates(conn)
    if video_max_dates:
        logger.info(f"Found {len(video_max_dates)} videos in database (incremental mode)")
    else:
        logger.info("No existing data found (full fetch mode)")

    # Fetch all videos metadata
    videos = fetch_all_active_videos(auth_mgr, proxies)
    if not videos:
        logger.error("No videos found")
        conn.close()
        return

    # Output paths
    suffix = date.today().isoformat()
    output_dir = get_output_dir()
    metadata_json = output_dir / f"video_metadata_{suffix}.json"
    summary_json = output_dir / f"video_summary_{suffix}.json"
    summary_csv = output_dir / f"vbrick_analytics_{suffix}.csv"

    # Save metadata JSON
    with open(metadata_json, "w") as mf:
        json.dump(videos, mf, indent=2)
    logger.info(f"Wrote metadata JSON to {metadata_json}")

    # Fetch analytics for each video
    end_date = date.today().isoformat()
    report_date = datetime.now().isoformat()
    all_rows = []
    summary_dict = {}

    # Default start date (2 years ago)
    default_start = (date.today() - timedelta(days=730)).isoformat()

    for video in tqdm(videos, desc="Fetching Analytics", unit="video"):
        video_id = video.get("id")
        when_uploaded = video.get("whenUploaded", "")[:10] or default_start

        # Calculate start date with overlap for incremental updates
        last_date = video_max_dates.get(video_id)
        start_date = calculate_overlap_start_date(last_date, when_uploaded, overlap_days)

        # Fetch analytics from start_date to end_date
        stats = get_video_summary(video_id, auth_mgr, start_date, end_date, proxies)
        summary_dict[video_id] = {"metadata": video, "dailySummary": stats}

        # Process into rows
        rows = process_video_analytics(video, stats, report_date)
        all_rows.extend(rows)

        # Batch upsert every 100 videos
        if len(all_rows) >= 1000:
            upsert_video_daily(conn, all_rows, logger)
            all_rows = []

    # Final upsert for remaining rows
    if all_rows:
        upsert_video_daily(conn, all_rows, logger)

    logger.info("Finished writing to DuckDB")

    # Save summary JSON
    with open(summary_json, "w") as jf:
        json.dump(summary_dict, jf, indent=2)
    logger.info(f"Wrote summary JSON to {summary_json}")

    # Write CSV if not disabled
    if not args.no_csv:
        # Re-fetch all rows for CSV (to include both new and existing data)
        csv_result = conn.execute("""
            SELECT * FROM vbrick_video_daily ORDER BY video_id, date
        """).fetchall()

        # Get column names
        columns = [desc[0] for desc in conn.description]

        # Convert to dicts
        db_rows = [dict(zip(columns, row)) for row in csv_result]
        csv_rows = convert_rows_for_csv(db_rows)

        if csv_rows:
            # Write CSV
            header = ['video_id', 'title', 'playbackUrl', 'duration', 'whenUploaded', 'lastViewed',
                     'whenPublished', 'commentCount', 'score', 'uploadedBy', 'tags', 'date', 'views',
                     'Desktop', 'Mobile', 'Other Device', 'Chrome', 'Microsoft Edge', 'Other Browser']

            with open(summary_csv, 'w', newline='', encoding='utf-8') as cf:
                writer = csv.DictWriter(cf, fieldnames=header)
                writer.writeheader()
                for row in csv_rows:
                    writer.writerow(row)
            logger.info(f"Wrote summary CSV to {summary_csv}")

    # Close database
    conn.close()

    # Optional: Copy to network location if configured
    network_source = cfg.get("network_source_path")
    network_dest = cfg.get("network_dest_path")

    if network_source and network_dest and not args.no_csv:
        source_path = f"{network_source}/vbrick_analytics_{suffix}.csv"
        destination_path = f"{network_dest}/vbrick_analytics.csv"
        try:
            shutil.move(source_path, destination_path)
            logger.info(f"File moved successfully from {source_path} to {destination_path}")
        except FileNotFoundError:
            logger.warning(f"The source file was not found: {source_path}")
        except Exception as e:
            logger.error(f"An error occurred while moving file: {e}")

    # Print final stats
    print_db_stats(logger=logger)


if __name__ == "__main__":
    main()
