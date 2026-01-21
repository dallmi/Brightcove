# Vbrick Analytics Pipeline

A comprehensive data processing pipeline that extracts, enriches, merges, and normalizes video analytics data from Vbrick. This suite combines video usage statistics with webcast attendance data to provide detailed insights into content performance across different dimensions such as geography, browsers, and devices.

## Overview

The Vbrick Analytics Pipeline is designed to help organizations understand how their video content is being consumed across different audiences and platforms. It pulls data from multiple Vbrick APIs, applies AI-driven categorization, and produces analysis-ready datasets in two distinct formats depending on your reporting needs.

### What This Pipeline Does

1. **Fetches Video Analytics**: Collects detailed viewing statistics for all active videos
2. **Processes Webcast Data**: Retrieves live webcast attendance and engagement metrics
3. **AI-Powered Categorization**: Automatically categorizes content using machine learning
4. **Data Integration**: Merges video and webcast data into unified datasets
5. **Persistent Storage**: Stores data in DuckDB for incremental updates and fast queries
6. **Flexible Output**: Produces both aggregated summaries and normalized data for different analysis needs

## Pipeline Components

### 1. Video Analytics Collection (`01_fetch_analytics.py`)
**Purpose**: Extracts comprehensive video viewing data from Vbrick API

**Key Functions**:
- Authenticates with Vbrick API using secure token management
- Fetches all active videos from the past 2 years
- Collects daily analytics including views, unique viewers, and watch time
- Groups data by device type (Desktop, Mobile) and browser (Chrome, Edge, etc.)
- Stores data in DuckDB with incremental updates (7-day overlap for API lag)

**Command Line Options**:
```bash
python 01_fetch_analytics.py           # Normal run (DuckDB + CSV)
python 01_fetch_analytics.py --stats   # Show database statistics
python 01_fetch_analytics.py --no-csv  # Skip CSV output
python 01_fetch_analytics.py --full    # Ignore checkpoint, fetch all data
python 01_fetch_analytics.py --overlap-days 14  # Custom overlap period
```

**Output**: `vbrick_analytics.csv` (CSV) + `vbrick_analytics.duckdb` (database)

| videoId | title | description | duration | views_total | browser_Chrome | browser_Edge | device_Desktop | device_Mobile |
|---------|-------|-------------|----------|-------------|----------------|--------------|----------------|---------------|
| v123 | Product Demo | Demo of new features | 1800 | 450 | 300 | 150 | 400 | 50 |
| v124 | Training Video | Employee onboarding | 3600 | 820 | 500 | 320 | 700 | 120 |

### 2. Webcast Data Processing (`02_Webcast.py`)
**Purpose**: Retrieves and enriches live webcast attendance data with AI categorization

**Key Functions**:
- Fetches webcast events and attendee sessions
- Maps attendee locations to geographical zones (APAC, Americas, EMEA, Swiss)
- Categorizes content using TF-IDF vectorization and K-means clustering
- Groups attendance by browser, device, and geographical zone
- Stores data in DuckDB with incremental updates

**Command Line Options**:
```bash
python 02_Webcast.py                    # Normal run (DuckDB + CSV)
python 02_Webcast.py --stats            # Show database statistics
python 02_Webcast.py --no-csv           # Skip CSV output
python 02_Webcast.py --full             # Ignore checkpoint, fetch all data
python 02_Webcast.py --start-date 2024-01-01  # Custom start date
```

**Output**: `webcast_summary.csv` (CSV) + data in `vbrick_analytics.duckdb`

| eventId | title | startDate | attendeeTotal | zone_APAC | zone_Americas | zone_EMEA | category | subcategory |
|---------|-------|-----------|---------------|-----------|---------------|-----------|----------|-------------|
| w789 | Global All-Hands | 2024-09-15 | 1250 | 300 | 600 | 350 | Corporate | Company Updates |
| w790 | Product Launch | 2024-09-20 | 850 | 200 | 400 | 250 | Product | Announcements |

### 3. Data Integration (`03_MergeWebcastVideo.py`)
**Purpose**: Combines video and webcast data into a unified dataset for comprehensive analysis

**Key Functions**:
- Matches webcast events with their corresponding recorded videos
- Merges attendance data with video viewing statistics
- Preserves both live event metrics and on-demand video consumption
- Creates comprehensive content performance overview
- **Can read directly from DuckDB for faster processing**

**Command Line Options**:
```bash
python 03_MergeWebcastVideo.py                # Use CSV input (default)
python 03_MergeWebcastVideo.py --from-duckdb  # Use DuckDB input (faster)
python 03_MergeWebcastVideo.py --no-format    # Skip regional number formatting
```

**Output**: `merged_webcast_video_summary.csv`

| id | title | eventURL | attendeeTotal | zone_APAC | zone_Americas | v_views | v_Chrome | v_Desktop | category |
|----|-------|----------|---------------|-----------|---------------|---------|----------|-----------|----------|
| w789 | Global All-Hands | https://vbrick.com/event/w789 | 1250 | 300 | 600 | 450 | 300 | 400 | Corporate |
| w790 | Product Launch | https://vbrick.com/event/w790 | 850 | 200 | 400 | 820 | 500 | 700 | Product |

### 4. Data Normalization (`04_NormalizedMergedWebcastVideo.py`)
**Purpose**: Transforms merged data into a normalized format optimized for dimensional analysis

**Key Functions**:
- Flattens multi-dimensional data into individual records per dimension
- Creates separate rows for each zone, browser, and device combination
- Enables easy filtering and analysis by specific dimensions
- Maintains data relationships while optimizing for analytics tools
- **Can read directly from DuckDB for faster processing**

**Command Line Options**:
```bash
python 04_NormalizedMergedWebcastVideo.py                # Use CSV input (default)
python 04_NormalizedMergedWebcastVideo.py --from-duckdb  # Use DuckDB input (faster)
python 04_NormalizedMergedWebcastVideo.py --no-format    # Skip regional number formatting
```

**Output**: `normalized_webcast_video_summary.csv`

| id | title | zone | webcast_browser | video_device | attendeeTotal | v_views | category |
|----|-------|------|-----------------|--------------|---------------|---------|----------|
| w789 | Global All-Hands | APAC | NULL | NULL | 300 | NULL | Corporate |
| w789 | Global All-Hands | Americas | NULL | NULL | 600 | NULL | Corporate |
| w789 | Global All-Hands | NULL | NULL | Desktop | NULL | 400 | Corporate |
| w790 | Product Launch | APAC | NULL | NULL | 200 | NULL | Product |

### 5. Migration Script (`migrate_vbrick_to_duckdb.py`)
**Purpose**: Migrate existing CSV data to DuckDB database

**Key Functions**:
- Imports existing video analytics CSV files
- Imports existing webcast summary CSV files
- Supports dry-run mode for preview
- Handles European number format (comma as decimal separator)

**Command Line Options**:
```bash
python migrate_vbrick_to_duckdb.py                     # Migrate all CSV files
python migrate_vbrick_to_duckdb.py --dry-run           # Preview without writing
python migrate_vbrick_to_duckdb.py --video-csv FILE    # Migrate specific video CSV
python migrate_vbrick_to_duckdb.py --webcast-csv FILE  # Migrate specific webcast CSV
python migrate_vbrick_to_duckdb.py --stats             # Show database statistics
```

## DuckDB Storage

The pipeline uses DuckDB for persistent storage, enabling incremental updates and faster queries.

### Database Schema

**Table: `vbrick_video_daily`**
- Primary Key: `(video_id, date)`
- Contains daily video analytics with device/browser breakdowns
- Source: `01_fetch_analytics.py`

**Table: `vbrick_webcasts`**
- Primary Key: `event_id`
- Contains webcast event data with attendance breakdowns by zone/browser/device
- Foreign Key: `vod_id` references video recordings
- Source: `02_Webcast.py`

### Querying the Database

You can query the DuckDB database directly using Python or the DuckDB CLI:

```python
import duckdb

# Connect to the database
conn = duckdb.connect('output/vbrick_analytics.duckdb')

# Example: Get total views per video
conn.execute("""
    SELECT video_id, title, SUM(views) as total_views
    FROM vbrick_video_daily
    GROUP BY video_id, title
    ORDER BY total_views DESC
    LIMIT 10
""").fetchdf()

# Example: Get webcast attendance by zone
conn.execute("""
    SELECT
        title,
        zone_apac as APAC,
        zone_america as Americas,
        zone_emea as EMEA,
        zone_swiss as Swiss
    FROM vbrick_webcasts
    ORDER BY start_date DESC
""").fetchdf()

conn.close()
```

### Incremental Updates

The pipeline implements incremental updates with a 7-day overlap:
- On first run, fetches all historical data
- On subsequent runs, only fetches data from `(last_date - 7_days)` to today
- The overlap handles API lag in reporting
- Use `--full` flag to force a complete re-fetch

## Real-World Use Cases

### Use Case 1: Regional Content Performance Analysis
**Scenario**: Understanding which content resonates in different global regions

**Data Source**: Normalized output or DuckDB query
```sql
-- Using DuckDB directly
SELECT zone, category, SUM(attendeeTotal) as total_attendance
FROM vbrick_webcasts
GROUP BY zone, category
ORDER BY total_attendance DESC
```

**Business Insight**: "Product announcements have 3x higher attendance in APAC region compared to EMEA"

### Use Case 2: Technology Adoption Tracking
**Scenario**: Monitoring browser and device usage trends for technical planning

**Data Source**: Merged output
```python
# Calculate mobile adoption rate
mobile_rate = df['device_Mobile'].sum() / (df['device_Desktop'].sum() + df['device_Mobile'].sum())
print(f"Mobile viewing accounts for {mobile_rate:.1%} of total consumption")
```

**Business Insight**: "Mobile viewing has increased to 25% of total consumption, indicating need for mobile-optimized content"

### Use Case 3: Content Lifecycle Analysis
**Scenario**: Comparing live webcast engagement vs. on-demand video consumption

**Data Source**: DuckDB query
```sql
-- Compare live attendance vs recorded views
SELECT
    w.title,
    w.attendee_total as live_attendance,
    SUM(v.views) as recorded_views,
    ROUND(SUM(v.views) * 1.0 / w.attendee_total, 2) as multiplier
FROM vbrick_webcasts w
LEFT JOIN vbrick_video_daily v ON w.vod_id = v.video_id
GROUP BY w.event_id, w.title, w.attendee_total
ORDER BY multiplier DESC
```

**Business Insight**: "Recorded content extends reach by 2.3x, justifying investment in high-quality recording infrastructure"

## Setup and Configuration

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure API Access

Create a `secrets.json` file with your Vbrick credentials:
```json
{
    "base_url": "https://your-vbrick-instance.com",
    "api_key": "your_api_key",
    "api_secret": "your_api_secret",
    "proxies": null,
    "output_dir": "./output",
    "duckdb": {
        "path": "output/vbrick_analytics.duckdb",
        "overlap_days": 7
    }
}
```

Alternatively, set the `VBRICK_CONFIG_JSON` environment variable to point to your config file.

### 3. Run the Pipeline

Execute scripts in sequence for full pipeline:
```bash
# Standard workflow (with DuckDB + CSV output)
python 01_fetch_analytics.py    # Collect video data
python 02_Webcast.py            # Process webcast data
python 03_MergeWebcastVideo.py --from-duckdb  # Combine datasets (fast mode)
python 04_NormalizedMergedWebcastVideo.py --from-duckdb  # Create normalized output
```

Or run individual scripts with specific options:
```bash
# Check database statistics
python 01_fetch_analytics.py --stats

# Force full re-fetch
python 01_fetch_analytics.py --full

# Skip CSV output (DuckDB only)
python 02_Webcast.py --no-csv
```

### 4. Migrate Existing Data (Optional)

If you have existing CSV files from previous runs:
```bash
python migrate_vbrick_to_duckdb.py --dry-run  # Preview migration
python migrate_vbrick_to_duckdb.py            # Execute migration
python migrate_vbrick_to_duckdb.py --stats    # Verify results
```

## Output File Selection Guide

**Use DuckDB (`vbrick_analytics.duckdb`) when**:
- Running ad-hoc SQL queries
- Need fast incremental updates
- Working with large datasets
- Building data pipelines

**Use Merged Output (`merged_webcast_video_summary.csv`) when**:
- Creating executive dashboards with aggregated metrics
- Analyzing overall content performance trends
- Building reports that need totals and summaries
- Working with BI tools that prefer wide-format data

**Use Normalized Output (`normalized_webcast_video_summary.csv`) when**:
- Performing dimensional analysis (by zone, browser, device)
- Creating detailed breakdowns and drill-down reports
- Using analytics tools that prefer long-format data
- Building visualizations that show distribution across categories

## System Requirements

- Python 3.7+
- Libraries: pandas, requests, scikit-learn, tqdm, duckdb
- Access to Vbrick API with analytics permissions
- Network access to shared drive (optional, configure paths in secrets.json)

## Troubleshooting

**Authentication Issues**: Verify API credentials in `secrets.json` file

**Large Datasets**: Scripts include progress bars and can handle thousands of records

**Network Paths**: Configure `input_dir`, `output_dir`, and network paths in secrets.json

**AI Categorization**: Categories are automatically generated; review and adjust clustering parameters if needed

**DuckDB Issues**:
- Check database exists: `python 01_fetch_analytics.py --stats`
- Reset and re-fetch: Delete `vbrick_analytics.duckdb` and run with `--full` flag
- Migration errors: Use `--dry-run` to preview before migrating
