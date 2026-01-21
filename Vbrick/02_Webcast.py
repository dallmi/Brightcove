"""
02_Webcast.py - Vbrick Webcast Attendance Fetcher

This script fetches webcast event data and attendance statistics from the Vbrick API.
It enriches the data with AI-powered categorization using TF-IDF and K-means clustering.

Features:
- Incremental updates (skips already-processed webcasts)
- DuckDB storage for persistent checkpointing
- AI categorization of webcast titles
- CSV output for backward compatibility
- Progress tracking with tqdm

Usage:
    python 02_Webcast.py              # Normal run (DuckDB + CSV)
    python 02_Webcast.py --stats      # Show database statistics
    python 02_Webcast.py --no-csv     # Skip CSV output
    python 02_Webcast.py --full       # Ignore checkpoint, fetch all data
"""

import argparse
import csv
import json
import logging
import shutil
import sys
from collections import Counter
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score
from tqdm import tqdm

from shared_vbrick import (
    VbrickAuthManager,
    safe_get,
    load_vbrick_config,
    init_vbrick_db,
    upsert_webcasts,
    get_existing_webcast_ids,
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


def fetch_webcasts(auth_mgr, start_date, end_date):
    """Fetch webcast events from the Vbrick API."""
    url = f"{auth_mgr.base_url}/api/v2/scheduled-events"
    headers = {"Authorization": f"Bearer {auth_mgr.get_token()}"}
    params = {
        "after": start_date,
        "before": end_date,
        "sortField": "startDate",
        "sortDirection": "asc"
    }
    data = safe_get(url, headers=headers, params=params, proxies=auth_mgr.proxies, logger=logger)
    return data if isinstance(data, list) else []


def fetch_attendance(auth_mgr, event_id):
    """Fetch attendance data for a webcast event."""
    base_url = f"{auth_mgr.base_url}/api/v2/scheduled-events/{event_id}/post-event-report"
    headers = {"Authorization": f"VBrick {auth_mgr.get_token()}"}
    all_sessions = []
    scroll_id = None
    page_count = 0
    max_pages = 40  # safety cap
    null_scroll_count = 0

    while True:
        params = {"scrollId": scroll_id} if scroll_id else {}
        data = safe_get(base_url, headers=headers, params=params, proxies=auth_mgr.proxies, logger=logger)
        if not data:
            return None

        sessions = data.get("sessions", [])
        if not sessions:
            break

        all_sessions.extend(sessions)

        scroll_id = data.get("scrollId")
        if scroll_id is None:
            null_scroll_count += 1
            if null_scroll_count >= 1:
                break
        else:
            null_scroll_count = 0

        page_count += 1
        if page_count >= max_pages:
            break

    data["sessions"] = all_sessions
    return data


def parse_duration_to_seconds(duration_str):
    """Parse HH:MM:SS duration string to seconds."""
    try:
        h, m, s = map(int, duration_str.split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return 0


def assign_categories_to_webcasts(webcast_data):
    """Use ML clustering to assign categories to webcast titles."""
    logger.info("Starting AI-based categorization of webcast titles...")

    titles = [item["title"] for item in webcast_data if "title" in item]
    if len(titles) < 2:
        logger.warning("Not enough titles for clustering, skipping categorization")
        for item in webcast_data:
            item["category_full"] = "Uncategorized"
        return

    logger.info(f"Extracted {len(titles)} titles for clustering.")

    custom_stop_words = list(text.ENGLISH_STOP_WORDS.union(['2024', '2025', '2026']))
    vectorizer = TfidfVectorizer(stop_words=custom_stop_words)
    X = vectorizer.fit_transform(titles)
    logger.info("TF-IDF vectorization complete.")

    # Find optimal cluster count
    best_k = 2
    best_score = -1
    logger.info("Evaluating optimal number of clusters...")
    for k in range(2, min(11, len(titles))):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)
        score = silhouette_score(X, labels)
        if score > best_score:
            best_k = k
            best_score = score
    logger.info(f"Optimal clusters: k={best_k}, silhouette={best_score:.4f}")

    # Fit final model
    kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X)

    # Extract top terms per cluster
    df = pd.DataFrame(X.todense()).groupby(clusters).mean()
    terms = vectorizer.get_feature_names_out()
    category_names = {}
    for i, row in df.iterrows():
        top_indices = np.argsort(row)[-3:][::-1]
        top_terms = [terms[idx] for idx in top_indices]
        category_names[i] = " / ".join(top_terms).title()

    # Assign categories
    for item, label in zip(webcast_data, clusters):
        item["category_full"] = category_names[label]

    logger.info("Categorization complete.")


def split_category_and_subcategory(webcast_data):
    """Split full category into category and subcategory."""
    for item in webcast_data:
        full_category = item.get("category_full", "")
        terms = full_category.split(" / ")
        item["category"] = terms[0] if terms else ""
        item["subcategory"] = full_category


# Zone/browser/device mappings
ZONE_MAPPING = {
    "APAC": "APAC",
    "APAC CS": "APAC",
    "APAC Cloud VDI's & Surface Device's": "APAC",
    "America": "America",
    "America CS": "America",
    "America Cloud VDI's & Surface Device's": "America",
    "Core HLS(Connect Me / Remote User)": "Other",
    "DefaultZone": "Other",
    "EMEA": "EMEA",
    "EMEA CS": "EMEA",
    "EMEA Cloud VDI's & Surface Device's": "EMEA",
    "None": "Other",
    "Secure Web Gateway Zone for Surface Device(Direct)": "Other",
    "Secure Web Gateway Zone for Surface Device(Direct).1": "Other",
    "Swiss": "Swiss",
    "Swiss CS": "Swiss",
    "Swiss Cloud VDI's & Surface Device's": "Swiss",
    "Card Center": "Other",
    "Z - Fallback": "Other"
}

BROWSER_MAPPING = {
    "Chrome": "Chrome",
    "Chrome mobile": "Chrome",
    "Chrome Mobile": "Chrome",
    "Microsoft Edge": "Edge",
    "Microsoft Edge mobile": "Edge",
    "Android WebView": "Other",
    "Apple Mail": "Other",
    "Firefox": "Other",
    "Mozilla": "Other",
    "None": "Other",
    "Opera": "Other",
    "Safari": "Other",
    "Safari mobile": "Other",
    "Unknown": "Other"
}

DEVICE_MAPPING = {
    "PC": "PC",
    "Mobile Device": "Mobile",
    "None": "Other",
    "Unknown": "Other"
}


def process_webcast_attendance(webcast, attendance, report_date):
    """
    Process webcast and attendance data into a row for DuckDB.

    Args:
        webcast: Webcast metadata dict
        attendance: Attendance data dict
        report_date: Report generation timestamp

    Returns:
        Dict ready for upsert to vbrick_webcasts
    """
    sessions = attendance.get("sessions", [])

    # Count by zone, browser, device
    zone_counter = Counter()
    browser_counter = Counter()
    device_counter = Counter()
    viewing_time = 0

    for session in sessions:
        # Zone
        raw_zone = session.get("zone")
        zone = str(raw_zone).strip() if raw_zone else "Other"
        grouped_zone = ZONE_MAPPING.get(zone, "Other")
        zone_counter[grouped_zone] += 1

        # Browser
        raw_browser = session.get("browser")
        browser = str(raw_browser).strip() if raw_browser else "Other"
        grouped_browser = BROWSER_MAPPING.get(browser, "Other")
        browser_counter[grouped_browser] += 1

        # Device
        raw_device = session.get("deviceType")
        device = str(raw_device).strip() if raw_device else "Other"
        grouped_device = DEVICE_MAPPING.get(device, "Other")
        device_counter[grouped_device] += 1

        # Viewing time
        viewing_time += parse_duration_to_seconds(session.get("viewingTime", "00:00:00"))

    attendee_total = sum(browser_counter.values())

    return {
        'event_id': webcast.get("id"),
        'title': webcast.get("title"),
        'vod_id': webcast.get("linkedVideoId"),
        'event_url': webcast.get("eventUrl"),
        'start_date': webcast.get("startDate"),
        'end_date': webcast.get("endDate"),
        'attendee_count': attendance.get("attendeeCount", 0),
        'attendee_total': attendee_total,
        'total_viewing_time': viewing_time,
        'zone_apac': zone_counter.get("APAC", 0),
        'zone_america': zone_counter.get("America", 0),
        'zone_emea': zone_counter.get("EMEA", 0),
        'zone_swiss': zone_counter.get("Swiss", 0),
        'zone_other': zone_counter.get("Other", 0),
        'browser_chrome': browser_counter.get("Chrome", 0),
        'browser_edge': browser_counter.get("Edge", 0),
        'browser_other': browser_counter.get("Other", 0),
        'device_pc': device_counter.get("PC", 0),
        'device_mobile': device_counter.get("Mobile", 0),
        'device_other': device_counter.get("Other", 0),
        'category': webcast.get("category", ""),
        'subcategory': webcast.get("subcategory", ""),
        'report_generated_on': report_date,
    }


def convert_row_for_csv(row):
    """Convert DuckDB row to CSV format for backward compatibility."""
    return {
        'id': row.get('event_id'),
        'title': row.get('title'),
        'vodId': row.get('vod_id'),
        'eventUrl': row.get('event_url'),
        'attendeeCount': row.get('attendee_count'),
        'attendeeTotal': row.get('attendee_total'),
        'startDate': row.get('start_date'),
        'endDate': row.get('end_date'),
        'total_viewingTime': row.get('total_viewing_time'),
        'category': row.get('category'),
        'subcategory': row.get('subcategory'),
        'zone_APAC': row.get('zone_apac'),
        'zone_America': row.get('zone_america'),
        'zone_EMEA': row.get('zone_emea'),
        'zone_Swiss': row.get('zone_swiss'),
        'zone_Other': row.get('zone_other'),
        'browser_Chrome': row.get('browser_chrome'),
        'browser_Edge': row.get('browser_edge'),
        'browser_Other': row.get('browser_other'),
        'deviceType_PC': row.get('device_pc'),
        'deviceType_Mobile': row.get('device_mobile'),
        'deviceType_Other': row.get('device_other'),
    }


def main():
    parser = argparse.ArgumentParser(description='Fetch Vbrick webcast attendance data')
    parser.add_argument('--stats', action='store_true', help='Show database statistics and exit')
    parser.add_argument('--no-csv', action='store_true', help='Skip CSV output')
    parser.add_argument('--full', action='store_true', help='Ignore checkpoint, fetch all data')
    parser.add_argument('--start-date', type=str, default="2025-07-01T00:00:00Z", help='Start date for fetching')
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

    if not all([base_url, api_key, api_secret]):
        logger.error("base_url, api_key, api_secret required in secrets.json")
        sys.exit(1)

    proxies = proxy_url if proxy_url else None
    auth_mgr = VbrickAuthManager(base_url, api_key, api_secret, proxies, logger=logger)

    # Initialize DuckDB
    conn = init_vbrick_db()
    logger.info("Initialized DuckDB database")

    # Get existing webcasts for incremental updates
    existing_ids = set() if args.full else get_existing_webcast_ids(conn)
    if existing_ids:
        logger.info(f"Found {len(existing_ids)} webcasts in database (incremental mode)")
    else:
        logger.info("No existing data found (full fetch mode)")

    # Fetch webcast metadata
    start_date = args.start_date
    end_date = datetime.now(timezone.utc).isoformat()

    logger.info(f"Fetching webcasts from {start_date} to {end_date}...")
    webcast_data = fetch_webcasts(auth_mgr, start_date, end_date)
    if not webcast_data:
        logger.error("No webcast data retrieved.")
        conn.close()
        return

    logger.info(f"Fetched {len(webcast_data)} webcasts")

    # Filter out already processed webcasts
    new_webcasts = [w for w in webcast_data if w.get("id") not in existing_ids]
    logger.info(f"Processing {len(new_webcasts)} new webcasts (skipping {len(webcast_data) - len(new_webcasts)} existing)")

    # AI categorization (on all webcasts for consistent clustering)
    assign_categories_to_webcasts(webcast_data)
    split_category_and_subcategory(webcast_data)

    # Output paths
    output_dir = get_output_dir()
    metadata_json = output_dir / "webcast_metadata_categorized.json"

    # Save metadata JSON
    with open(metadata_json, "w", encoding="utf-8") as jf:
        json.dump(webcast_data, jf, indent=2)
    logger.info(f"Webcast metadata written to {metadata_json}")

    # Process webcasts and fetch attendance
    report_date = datetime.now().isoformat()
    rows = []
    failed_events = []

    # Build lookup for categorized data
    webcast_lookup = {w.get("id"): w for w in webcast_data}

    for webcast in tqdm(new_webcasts, desc="Processing Webcasts", unit="webcast"):
        event_id = webcast.get("id")
        title = webcast.get("title")

        # Get categorized version
        categorized = webcast_lookup.get(event_id, webcast)

        attendance = fetch_attendance(auth_mgr, event_id)
        if attendance is None:
            failed_events.append({"id": event_id, "title": title})
            continue

        row = process_webcast_attendance(categorized, attendance, report_date)
        rows.append(row)

    # Upsert to DuckDB
    if rows:
        upsert_webcasts(conn, rows, logger)
        logger.info(f"Upserted {len(rows)} webcasts to DuckDB")

    # Log failed events
    if failed_events:
        failed_csv = output_dir / "failed_webcasts.csv"
        with open(failed_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerows(failed_events)
        logger.info(f"{len(failed_events)} webcasts failed and logged to {failed_csv}")

    # Write CSV if not disabled
    if not args.no_csv:
        # Re-fetch all rows for CSV (to include both new and existing data)
        csv_result = conn.execute("""
            SELECT * FROM vbrick_webcasts ORDER BY start_date, event_id
        """).fetchall()

        columns = [desc[0] for desc in conn.description]
        db_rows = [dict(zip(columns, row)) for row in csv_result]
        csv_rows = [convert_row_for_csv(row) for row in db_rows]

        if csv_rows:
            summary_csv = output_dir / "webcast_summary.csv"
            header = [
                'id', 'title', 'vodId', 'eventUrl', 'attendeeCount', 'attendeeTotal',
                'startDate', 'endDate', 'total_viewingTime', 'category', 'subcategory',
                'zone_APAC', 'zone_America', 'zone_EMEA', 'zone_Swiss', 'zone_Other',
                'browser_Chrome', 'browser_Edge', 'browser_Other',
                'deviceType_PC', 'deviceType_Mobile', 'deviceType_Other'
            ]

            with open(summary_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                for row in csv_rows:
                    writer.writerow({k: row.get(k, "") for k in header})
            logger.info(f"Webcast summary exported to {summary_csv}")

    # Close database
    conn.close()

    # Optional: Copy to network location
    network_source = cfg.get("network_source_path")
    network_dest = cfg.get("network_dest_path")

    if network_source and network_dest and not args.no_csv:
        source_path = f"{network_source}/webcast_summary.csv"
        destination_path = f"{network_dest}/webcast_summary.csv"
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
