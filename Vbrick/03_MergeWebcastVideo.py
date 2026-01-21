"""
03_MergeWebcastVideo.py - Merge Video and Webcast Data

This script merges video analytics data with webcast metadata to create a unified summary report.

Features:
- Can read from DuckDB or CSV files
- SQL-based merge when using DuckDB (faster)
- Regional number formatting
- CSV output for downstream consumption

Usage:
    python 03_MergeWebcastVideo.py                # Use CSV input (default)
    python 03_MergeWebcastVideo.py --from-duckdb  # Use DuckDB input (faster)
    python 03_MergeWebcastVideo.py --no-format    # Skip regional formatting
"""

import argparse
import logging
import os
import shutil

import pandas as pd

from shared_vbrick import (
    load_vbrick_config,
    init_vbrick_db,
    get_output_dir,
    get_vbrick_db_path,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def merge_from_duckdb(conn):
    """
    Perform merge using SQL queries on DuckDB.

    This is more efficient than loading CSVs and merging in pandas.
    """
    logger.info("Merging data using DuckDB SQL...")

    # Aggregate video data and join with webcasts
    query = """
        SELECT
            w.event_id as id,
            w.title,
            w.vod_id as vodId,
            w.event_url as eventUrl,
            w.attendee_count as attendeeCount,
            w.attendee_total as attendeeTotal,
            w.start_date as startDate,
            w.end_date as endDate,
            w.total_viewing_time as total_viewingTime,
            w.category,
            w.subcategory,
            w.zone_apac as zone_APAC,
            w.zone_america as zone_America,
            w.zone_emea as zone_EMEA,
            w.zone_swiss as zone_Swiss,
            w.zone_other as zone_Other,
            w.browser_chrome as browser_Chrome,
            w.browser_edge as browser_Edge,
            w.browser_other as browser_Other,
            w.device_pc as deviceType_PC,
            w.device_mobile as deviceType_Mobile,
            w.device_other as deviceType_Other,
            v.duration as v_duration,
            v.last_viewed as v_lastViewed,
            v.when_published as v_whenPublished,
            v.views as v_views,
            v.device_desktop as v_Desktop,
            v.device_mobile as v_Mobile,
            v.device_other as "v_Other Device",
            v.browser_chrome as v_Chrome,
            v.browser_edge as "v_Microsoft Edge",
            v.browser_other as "v_Other Browser"
        FROM vbrick_webcasts w
        LEFT JOIN (
            SELECT
                video_id,
                MAX(duration) as duration,
                MAX(last_viewed) as last_viewed,
                MAX(when_published) as when_published,
                SUM(views) as views,
                SUM(device_desktop) as device_desktop,
                SUM(device_mobile) as device_mobile,
                SUM(device_other) as device_other,
                SUM(browser_chrome) as browser_chrome,
                SUM(browser_edge) as browser_edge,
                SUM(browser_other) as browser_other
            FROM vbrick_video_daily
            GROUP BY video_id
        ) v ON w.vod_id = v.video_id
        ORDER BY w.start_date, w.event_id
    """

    df = conn.execute(query).fetchdf()
    logger.info(f"Merged {len(df)} rows from DuckDB")
    return df


def merge_from_csv(video_csv, webcast_csv):
    """
    Perform merge using pandas on CSV files.

    This is the original merge logic, preserved for backward compatibility.
    """
    logger.info(f"Loading video data from {video_csv}")
    df_video = pd.read_csv(video_csv)

    # Drop the "date" column since it's used only for daily breakdowns
    if 'date' in df_video.columns:
        df_video = df_video.drop(columns=['date'])

    # Dynamically identify all columns except 'video_id' and 'views'
    non_summed_columns = [col for col in df_video.columns if col not in ['video_id', 'views']]

    # Aggregate: sum views, take first occurrence of all other columns
    aggregated_df = df_video.groupby("video_id").agg({
        "views": 'sum',
        **{col: "first" for col in non_summed_columns}
    }).reset_index()

    logger.info(f"Loading webcast data from {webcast_csv}")
    df_webcast = pd.read_csv(webcast_csv)

    # Identify numeric columns to keep, excluding specific ones
    excluded_columns = {'video_id', 'commentCount', 'score'}
    numeric_columns = aggregated_df.select_dtypes(include='number').columns
    columns_to_keep = ['video_id', 'duration', 'lastViewed', 'whenPublished', 'views']
    columns_to_keep += [col for col in numeric_columns if col not in excluded_columns and col not in columns_to_keep]

    # Filter and rename video columns
    available_columns = [col for col in columns_to_keep if col in aggregated_df.columns]
    aggregated_df = aggregated_df[available_columns]
    aggregated_df = aggregated_df.rename(columns={col: f'v_{col}' for col in aggregated_df.columns if col != 'video_id'})

    # Merge using a left join
    merged_df = df_webcast.merge(aggregated_df, how='left', left_on='vodId', right_on='video_id')

    # Drop the redundant join key from video data
    if 'video_id' in merged_df.columns:
        merged_df = merged_df.drop(columns=['video_id'])

    logger.info(f"Merged {len(merged_df)} rows from CSV")
    return merged_df


def format_number(val):
    """Format numeric values for regional display (European format)."""
    if isinstance(val, float) and val.is_integer():
        return str(int(val)).replace('.', ',')
    elif isinstance(val, (float, int)):
        return str(val).replace('.', ',')
    return val


def main():
    parser = argparse.ArgumentParser(description='Merge video and webcast data')
    parser.add_argument('--from-duckdb', action='store_true', help='Read from DuckDB instead of CSV')
    parser.add_argument('--no-format', action='store_true', help='Skip regional number formatting')
    parser.add_argument('--video-csv', type=str, help='Path to video analytics CSV (overrides config)')
    parser.add_argument('--webcast-csv', type=str, help='Path to webcast summary CSV (overrides config)')
    args = parser.parse_args()

    # Load configuration
    try:
        cfg = load_vbrick_config()
    except FileNotFoundError:
        cfg = {}
        logger.warning("Config file not found, using defaults")

    output_dir = get_output_dir()

    if args.from_duckdb:
        # Use DuckDB
        db_path = get_vbrick_db_path()
        if not db_path.exists():
            logger.error(f"DuckDB not found at {db_path}. Run 01_fetch_analytics.py and 02_Webcast.py first.")
            return

        conn = init_vbrick_db()
        merged_df = merge_from_duckdb(conn)
        conn.close()
    else:
        # Use CSV files
        input_dir = cfg.get("input_dir", str(output_dir))

        video_csv = args.video_csv or os.path.join(input_dir, cfg.get("video_analytics_file", "vbrick_analytics.csv"))
        webcast_csv = args.webcast_csv or os.path.join(input_dir, cfg.get("webcast_summary_file", "webcast_summary.csv"))

        if not os.path.exists(video_csv):
            # Try output directory
            video_csv = output_dir / "vbrick_analytics.csv"
            # Find most recent
            video_files = list(output_dir.glob("vbrick_analytics_*.csv"))
            if video_files:
                video_csv = max(video_files, key=lambda f: f.stat().st_mtime)

        if not os.path.exists(webcast_csv):
            webcast_csv = output_dir / "webcast_summary.csv"

        if not os.path.exists(video_csv):
            logger.error(f"Video CSV not found: {video_csv}")
            return
        if not os.path.exists(webcast_csv):
            logger.error(f"Webcast CSV not found: {webcast_csv}")
            return

        merged_df = merge_from_csv(video_csv, webcast_csv)

    # Replace NaN values with 0
    merged_df = merged_df.fillna(0)

    # Apply regional formatting if not disabled
    if not args.no_format:
        merged_df = merged_df.apply(lambda col: col.map(format_number) if col.dtype != "object" else col)

    # Save the result
    output_file = output_dir / "merged_webcast_video_summary.csv"
    merged_df.to_csv(output_file, index=False)
    logger.info(f"Join complete. Output saved to {output_file}")

    # Optional: Copy to network location if configured
    network_source = cfg.get("network_source_path")
    network_dest = cfg.get("network_dest_path")

    if network_source and network_dest:
        source_path = f"{network_source}/merged_webcast_video_summary.csv"
        destination_path = f"{network_dest}/merged_webcast_video_summary.csv"
        try:
            shutil.move(source_path, destination_path)
            logger.info(f"File moved successfully from {source_path} to {destination_path}")
        except FileNotFoundError:
            logger.warning(f"The source file was not found: {source_path}")
        except Exception as e:
            logger.error(f"An error occurred while moving file: {e}")


if __name__ == "__main__":
    main()
