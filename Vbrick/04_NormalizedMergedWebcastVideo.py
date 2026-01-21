"""
04_NormalizedMergedWebcastVideo.py - Normalize Webcast Video Summary Data

This script processes a merged webcast video summary and transforms it into a normalized format
suitable for analysis by dimension (e.g., zone, browser, device).

Features:
- Can read from DuckDB or CSV files
- Flattens data by dimension for easier analysis
- Regional number formatting support

Usage:
    python 04_NormalizedMergedWebcastVideo.py                # Use CSV input (default)
    python 04_NormalizedMergedWebcastVideo.py --from-duckdb  # Use DuckDB input (faster)
    python 04_NormalizedMergedWebcastVideo.py --no-format    # Skip regional formatting

Step-by-Step Process:

1. **Load Data**:
   - Read the merged data from either DuckDB (via SQL join) or CSV file.

2. **Define Metadata Columns**:
   - Specify the key metadata fields to retain for each webcast record.

3. **Define Dimension Configurations**:
   - Set up configurations for each dimension (zone, browser, device) including:
     - The output column name.
     - The source columns to check.
     - The labels to assign.
     - The metric column to populate (e.g., attendeeTotal, v_views).

4. **Transform Data**:
   - Iterate through each row of the dataset.
   - For each dimension, check if the value is non-zero.
   - If so, create a new record with:
     - Metadata fields.
     - One-hot encoded dimension label.
     - Corresponding metric value.

Example 1: Webcast Zone Breakdown
---------------------------------
Original Row:
id | title | zone_APAC | zone_America | zone_Swiss | attendeeTotal
1  | AI Talk | 5         | 10           | 3          | 18

Transformed Rows:
id | title   | zone    | attendeeTotal
1  | AI Talk | APAC    | 5
1  | AI Talk | America | 10
1  | AI Talk | Swiss   | 3

Example 2: Video Browser Views
Original Row:
id | title          | v_Chrome | v_Microsoft Edge | v_Other Browser | v_views
1  | Data Deep Dive | 100      | 50               | 25              | 175

Transformed Rows:
id | title          | browser      | v_views
1  | Data Deep Dive | Chrome       | 100
1  | Data Deep Dive | Edge         | 50
1  | Data Deep Dive | Other        | 25

5. **Export Result**:
   - Save the normalized data to normalized_webcast_video_summary.csv.

Output: A flattened CSV file where each row represents a single dimension-metric combination for a webcast.
"""

import argparse
import logging
import os

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

# Define metadata columns to retain (matching DuckDB column names)
METADATA_COLS_DUCKDB = [
    "id", "title", "vodId", "eventUrl", "startDate", "endDate",
    "total_viewingTime", "category", "subcategory", "v_duration", "v_lastViewed", "v_whenPublished"
]

# Define metadata columns for CSV (may have different casing)
METADATA_COLS_CSV = [
    "id", "title", "vodID", "eventURL", "startDate", "endDate",
    "total_viewingTime", "category", "subcategory", "v_duration", "v_lastViewed", "v_whenPublished"
]


def get_dimension_configs_duckdb():
    """Dimension configurations for DuckDB column names."""
    return [
        {
            "dimension_column": "zone",
            "columns": ["zone_APAC", "zone_America", "zone_EMEA", "zone_Other", "zone_Swiss"],
            "labels": ["APAC", "America", "EMEA", "Other", "Swiss"],
            "metric_column": "attendeeTotal"
        },
        {
            "dimension_column": "webcast_browser",
            "columns": ["browser_Chrome", "browser_Edge", "browser_Other"],
            "labels": ["Chrome", "Edge", "Other"],
            "metric_column": "attendeeTotal"
        },
        {
            "dimension_column": "webcast_device",
            "columns": ["deviceType_Mobile", "deviceType_Other", "deviceType_PC"],
            "labels": ["Mobile", "Other", "PC"],
            "metric_column": "attendeeTotal"
        },
        {
            "dimension_column": "video_browser",
            "columns": ["v_Chrome", "v_Microsoft Edge", "v_Other Browser"],
            "labels": ["Chrome", "Microsoft Edge", "Other"],
            "metric_column": "v_views"
        },
        {
            "dimension_column": "video_device",
            "columns": ["v_Desktop", "v_Mobile", "v_Other Device"],
            "labels": ["Desktop", "Mobile", "Other"],
            "metric_column": "v_views"
        }
    ]


def get_dimension_configs_csv():
    """Dimension configurations for CSV column names (may have different naming)."""
    return [
        {
            "dimension_column": "zone",
            "columns": ["zone_APAC", "zone_America", "zone_EMEA", "zone_Other", "zone_Swiss"],
            "labels": ["APAC", "America", "EMEA", "Other", "Swiss"],
            "metric_column": "attendeeTotal"
        },
        {
            "dimension_column": "webcast_browser",
            "columns": ["browser_Chrome", "browser_Edge", "browser_Other"],
            "labels": ["Chrome", "Edge", "Other"],
            "metric_column": "attendeeTotal"
        },
        {
            "dimension_column": "webcast_device",
            "columns": ["deviceType_Mobile", "deviceType_Other", "deviceType_PC"],
            "labels": ["Mobile", "Other", "PC"],
            "metric_column": "attendeeTotal"
        },
        {
            "dimension_column": "video_browser",
            "columns": ["v_Chrome", "v_Microsoft Edge", "v_browser Other"],
            "labels": ["Chrome", "Microsoft Edge", "Other"],
            "metric_column": "v_views"
        },
        {
            "dimension_column": "video_device",
            "columns": ["v_Desktop", "v_Mobile", "v_device Other"],
            "labels": ["Desktop", "Mobile", "Other"],
            "metric_column": "v_views"
        }
    ]


def load_from_duckdb(conn):
    """
    Load merged data directly from DuckDB using SQL join.

    This replicates the merge logic from 03_MergeWebcastVideo.py.
    """
    logger.info("Loading merged data from DuckDB...")

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
    logger.info(f"Loaded {len(df)} rows from DuckDB")
    return df


def load_from_csv(csv_path):
    """Load merged data from CSV file."""
    logger.info(f"Loading merged data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from CSV")
    return df


def normalize_data(df, metadata_cols, dimension_configs):
    """
    Transform data into normalized format by dimension.

    Args:
        df: DataFrame with merged webcast/video data
        metadata_cols: List of metadata columns to retain
        dimension_configs: List of dimension configuration dicts

    Returns:
        DataFrame with normalized records
    """
    logger.info("Normalizing data by dimensions...")

    # Filter metadata columns to only those present in the dataframe
    available_metadata = [col for col in metadata_cols if col in df.columns]
    if len(available_metadata) < len(metadata_cols):
        missing = set(metadata_cols) - set(available_metadata)
        logger.warning(f"Some metadata columns not found: {missing}")

    records = []

    for _, row in df.iterrows():
        base = row[available_metadata].to_dict()

        for config in dimension_configs:
            for col, label in zip(config["columns"], config["labels"]):
                # Check if column exists and value is non-zero
                if col in df.columns and row[col] != 0:
                    record = base.copy()
                    # Initialize all dimension columns to None
                    record.update({
                        "zone": None,
                        "webcast_browser": None,
                        "webcast_device": None,
                        "video_browser": None,
                        "video_device": None,
                        "attendeeTotal": None,
                        "v_views": None
                    })
                    # Set the current dimension value
                    record[config["dimension_column"]] = label
                    # Set the appropriate metric value
                    record[config["metric_column"]] = row[col]
                    records.append(record)

    normalized_df = pd.DataFrame(records)
    logger.info(f"Created {len(normalized_df)} normalized records")
    return normalized_df


def format_number(val):
    """Format numeric values for regional display (European format)."""
    if isinstance(val, float) and val.is_integer():
        return str(int(val)).replace('.', ',')
    elif isinstance(val, (float, int)):
        return str(val).replace('.', ',')
    return val


def main():
    parser = argparse.ArgumentParser(description='Normalize merged webcast video data')
    parser.add_argument('--from-duckdb', action='store_true', help='Read from DuckDB instead of CSV')
    parser.add_argument('--no-format', action='store_true', help='Skip regional number formatting')
    parser.add_argument('--input-csv', type=str, help='Path to merged CSV (overrides config)')
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
        df = load_from_duckdb(conn)
        conn.close()

        # Use DuckDB column configurations
        metadata_cols = METADATA_COLS_DUCKDB
        dimension_configs = get_dimension_configs_duckdb()
    else:
        # Use CSV file
        input_dir = cfg.get("input_dir", str(output_dir))

        input_csv = args.input_csv or os.path.join(
            input_dir, cfg.get("merged_summary_file", "merged_webcast_video_summary.csv")
        )

        if not os.path.exists(input_csv):
            # Try output directory
            input_csv = output_dir / "merged_webcast_video_summary.csv"

        if not os.path.exists(input_csv):
            logger.error(f"Input CSV not found: {input_csv}")
            return

        df = load_from_csv(input_csv)

        # Use CSV column configurations
        metadata_cols = METADATA_COLS_CSV
        dimension_configs = get_dimension_configs_csv()

    # Replace NaN values with 0
    df = df.fillna(0)

    # Normalize the data
    normalized_df = normalize_data(df, metadata_cols, dimension_configs)

    # Apply regional formatting if not disabled
    if not args.no_format:
        normalized_df = normalized_df.apply(
            lambda col: col.map(format_number) if col.dtype != "object" else col
        )

    # Save the result
    output_file = output_dir / "normalized_webcast_video_summary.csv"
    normalized_df.to_csv(output_file, index=False)
    logger.info(f"Normalized data exported to '{output_file}' with {len(normalized_df)} records")


if __name__ == "__main__":
    main()
