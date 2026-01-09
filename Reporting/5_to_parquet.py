"""
5_to_parquet.py - Convert Brightcove Reporting CSVs to Parquet format

Purpose:
    Converts daily analytics CSV files to Parquet format for efficient PowerBI reporting.
    Parquet provides better compression, faster queries, and proper data typing.

Runtime: ~30 seconds depending on data size

Input:
    - CSV files from Script 3 (daily_analytics_summary*.csv) or Script 4 (concatenated files)
    - Located in: ./2024/, ./2025/, or Q:/Brightcove/Reporting/

Output:
    - Parquet files with proper schema and compression
    - Output location configurable (local or network drive)

Requirements:
    pip install pandas pyarrow
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input/Output paths - adjust as needed
INPUT_DIR = Path(__file__).parent  # Current Reporting folder
OUTPUT_DIR = Path(__file__).parent / "parquet"  # Local parquet output

# Alternative: Network drive output for PowerBI
# OUTPUT_DIR = Path("Q:/Brightcove/Reporting/parquet")

# Years to process
YEARS = ["2024", "2025"]

# File patterns to convert
FILE_PATTERNS = {
    "internet": "daily_analytics_summary_*_to_*.csv",
    "research": "daily_analytics_summary_research_*_to_*.csv",
    "concatenated_internet": "daily_analytics_*_internet.csv",
    "concatenated_research": "daily_analytics_*_research.csv",
}

# Compression options: 'snappy' (fast), 'gzip' (smaller), 'zstd' (balanced)
COMPRESSION = "snappy"

# Partitioning - enables faster filtered queries in PowerBI
# Options: None, ["channel"], ["channel", "year"], ["year", "month"]
PARTITION_COLS = None  # Set to ["channel"] for partitioned output

# =============================================================================
# SCHEMA DEFINITION - Ensures proper data types for PowerBI
# =============================================================================

# Define explicit data types for each column
COLUMN_DTYPES = {
    # Identifiers (strings)
    "channel": "string",
    "account_id": "string",
    "video_id": "string",
    "name": "string",
    "reference_id": "string",
    "original_filename": "string",
    "created_by": "string",

    # Date columns (will be converted to datetime)
    "date": "datetime64[ns]",
    "created_at": "datetime64[ns]",
    "published_at": "datetime64[ns]",
    "report_generated_on": "datetime64[ns]",

    # Integer metrics (nullable Int64 for potential nulls)
    "video_view": "Int64",
    "video_impression": "Int64",
    "video_seconds_viewed": "Int64",
    "views_desktop": "Int64",
    "views_mobile": "Int64",
    "views_tablet": "Int64",
    "views_other": "Int64",
    "video_duration": "Int64",

    # Float metrics (engagement scores and rates)
    "play_rate": "float64",
    "engagement_score": "float64",
    "video_engagement_1": "float64",
    "video_engagement_25": "float64",
    "video_engagement_50": "float64",
    "video_engagement_75": "float64",
    "video_engagement_100": "float64",
    "video_percent_viewed": "float64",

    # Categorical/text fields
    "video_content_type": "string",
    "video_length": "string",
    "video_category": "string",
    "country": "string",
    "language": "string",
    "business_unit": "string",
    "tags": "string",
}

# PyArrow schema for explicit typing
PYARROW_SCHEMA = pa.schema([
    ("channel", pa.string()),
    ("account_id", pa.string()),
    ("video_id", pa.string()),
    ("name", pa.string()),
    ("date", pa.timestamp("ns")),
    ("video_view", pa.int64()),
    ("video_impression", pa.int64()),
    ("play_rate", pa.float64()),
    ("engagement_score", pa.float64()),
    ("video_engagement_1", pa.float64()),
    ("video_engagement_25", pa.float64()),
    ("video_engagement_50", pa.float64()),
    ("video_engagement_75", pa.float64()),
    ("video_engagement_100", pa.float64()),
    ("video_percent_viewed", pa.float64()),
    ("video_seconds_viewed", pa.int64()),
    ("views_desktop", pa.int64()),
    ("views_mobile", pa.int64()),
    ("views_tablet", pa.int64()),
    ("views_other", pa.int64()),
    ("created_at", pa.timestamp("ns")),
    ("published_at", pa.timestamp("ns")),
    ("original_filename", pa.string()),
    ("created_by", pa.string()),
    ("tags", pa.string()),
    ("reference_id", pa.string()),
    ("video_content_type", pa.string()),
    ("video_length", pa.string()),
    ("video_duration", pa.int64()),
    ("video_category", pa.string()),
    ("country", pa.string()),
    ("language", pa.string()),
    ("business_unit", pa.string()),
    ("report_generated_on", pa.timestamp("ns")),
])


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def find_csv_files(base_dir: Path, years: list) -> list:
    """
    Find all daily analytics CSV files in year folders and base directory.
    """
    csv_files = []

    # Search in year folders (output from Script 3)
    for year in years:
        year_dir = base_dir / year
        if year_dir.exists():
            for csv_file in year_dir.glob("daily_analytics_*.csv"):
                csv_files.append(csv_file)

    # Search in base directory (output from Script 4 or manual placement)
    for csv_file in base_dir.glob("daily_analytics_*.csv"):
        csv_files.append(csv_file)

    return list(set(csv_files))  # Remove duplicates


def apply_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply proper data types to DataFrame columns.
    Handles missing columns gracefully.
    """
    for col, dtype in COLUMN_DTYPES.items():
        if col not in df.columns:
            continue

        try:
            if "datetime" in dtype:
                # Parse dates with error handling
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "string":
                # Convert to string, handling NaN
                df[col] = df[col].astype("string")
            elif dtype == "Int64":
                # Nullable integer type
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        except Exception as e:
            print(f"  Warning: Could not convert column '{col}' to {dtype}: {e}")

    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add useful derived columns for PowerBI reporting.
    """
    if "date" in df.columns and df["date"].notna().any():
        # Extract year/month for partitioning and filtering
        df["year"] = df["date"].dt.year.astype("Int64")
        df["month"] = df["date"].dt.month.astype("Int64")
        df["year_month"] = df["date"].dt.to_period("M").astype("string")
        df["day_of_week"] = df["date"].dt.day_name().astype("string")

    # Total device views (if device columns exist)
    device_cols = ["views_desktop", "views_mobile", "views_tablet", "views_other"]
    if all(col in df.columns for col in device_cols):
        df["views_total_devices"] = (
            df["views_desktop"].fillna(0) +
            df["views_mobile"].fillna(0) +
            df["views_tablet"].fillna(0) +
            df["views_other"].fillna(0)
        ).astype("Int64")

    return df


def convert_csv_to_parquet(
    csv_path: Path,
    output_dir: Path,
    compression: str = "snappy",
    partition_cols: list = None
) -> Path:
    """
    Convert a single CSV file to Parquet format.

    Args:
        csv_path: Path to input CSV file
        output_dir: Directory for output Parquet file
        compression: Compression algorithm ('snappy', 'gzip', 'zstd')
        partition_cols: List of columns to partition by (optional)

    Returns:
        Path to output Parquet file/directory
    """
    print(f"\nProcessing: {csv_path.name}")

    # Read CSV with low_memory=False to avoid dtype warnings
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")

    # Apply proper data types
    df = apply_dtypes(df)

    # Add derived columns for better PowerBI experience
    df = add_derived_columns(df)

    # Generate output filename
    output_name = csv_path.stem + ".parquet"
    output_path = output_dir / output_name

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    if partition_cols:
        # Partitioned output (creates directory structure)
        # Useful for very large datasets
        partition_path = output_dir / csv_path.stem
        pq.write_to_dataset(
            pa.Table.from_pandas(df),
            root_path=str(partition_path),
            partition_cols=partition_cols,
            compression=compression,
        )
        print(f"  Output (partitioned): {partition_path}")
        return partition_path
    else:
        # Single file output
        df.to_parquet(
            output_path,
            engine="pyarrow",
            compression=compression,
            index=False,
        )

        # Report file size comparison
        csv_size = csv_path.stat().st_size / (1024 * 1024)  # MB
        parquet_size = output_path.stat().st_size / (1024 * 1024)  # MB
        compression_ratio = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0

        print(f"  CSV size: {csv_size:.2f} MB")
        print(f"  Parquet size: {parquet_size:.2f} MB")
        print(f"  Compression: {compression_ratio:.1f}% smaller")
        print(f"  Output: {output_path}")

        return output_path


def combine_and_convert(
    csv_files: list,
    output_path: Path,
    compression: str = "snappy"
) -> Path:
    """
    Combine multiple CSV files into a single Parquet file.
    Useful for creating a unified dataset from multiple years/categories.
    """
    print(f"\nCombining {len(csv_files)} files into single Parquet...")

    dfs = []
    for csv_path in csv_files:
        df = pd.read_csv(csv_path, low_memory=False)
        df = apply_dtypes(df)
        df = add_derived_columns(df)
        # Add source file info for traceability
        df["source_file"] = csv_path.name
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"  Total rows: {len(combined_df):,}")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    combined_df.to_parquet(
        output_path,
        engine="pyarrow",
        compression=compression,
        index=False,
    )

    print(f"  Output: {output_path}")
    return output_path


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main conversion workflow.
    """
    print("=" * 60)
    print("Brightcove Reporting - CSV to Parquet Conversion")
    print("=" * 60)
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Compression: {COMPRESSION}")
    print(f"Partition columns: {PARTITION_COLS or 'None (single file)'}")

    # Find all CSV files to convert
    csv_files = find_csv_files(INPUT_DIR, YEARS)

    if not csv_files:
        print("\nNo CSV files found to convert!")
        print("Expected locations:")
        for year in YEARS:
            print(f"  - {INPUT_DIR / year}/daily_analytics_*.csv")
        print(f"  - {INPUT_DIR}/daily_analytics_*.csv")
        return

    print(f"\nFound {len(csv_files)} CSV file(s) to convert:")
    for f in csv_files:
        print(f"  - {f.name}")

    # Convert each file individually
    converted_files = []
    for csv_file in csv_files:
        try:
            output_path = convert_csv_to_parquet(
                csv_file,
                OUTPUT_DIR,
                compression=COMPRESSION,
                partition_cols=PARTITION_COLS,
            )
            converted_files.append(output_path)
        except Exception as e:
            print(f"  ERROR converting {csv_file.name}: {e}")

    # Optionally: Create a combined "master" Parquet file
    # Uncomment below if you want all data in one file for PowerBI

    # if len(csv_files) > 1:
    #     combined_output = OUTPUT_DIR / "brightcove_analytics_combined.parquet"
    #     combine_and_convert(csv_files, combined_output, COMPRESSION)

    print("\n" + "=" * 60)
    print("Conversion complete!")
    print(f"Parquet files saved to: {OUTPUT_DIR}")
    print("\nPowerBI Import Instructions:")
    print("  1. Open PowerBI Desktop")
    print("  2. Get Data > Parquet")
    print("  3. Navigate to the parquet folder and select file(s)")
    print("  4. The data will load with proper types and compression")
    print("=" * 60)


if __name__ == "__main__":
    main()
