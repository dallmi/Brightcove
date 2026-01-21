# CrossPlatformAnalytics

A unified analytics database that combines video analytics data from both **Vbrick** (internal video platform) and **Brightcove** (external video platform) for cross-platform reporting and comparison.

## Overview

CrossPlatformAnalytics syncs data from two separate DuckDB databases into a single unified database, enabling:

- Cross-platform view comparisons
- Device usage analysis across platforms
- Unified reporting dashboards
- Webcast performance analysis (Vbrick-specific)

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│  Vbrick Pipeline    │     │  UnifiedPipeline    │
│  vbrick_analytics   │     │  analytics.duckdb   │
│    .duckdb          │     │                     │
└─────────┬───────────┘     └─────────┬───────────┘
          │                           │
          │  sync_vbrick.py           │  sync_brightcove.py
          │                           │
          v                           v
      ┌───────────────────────────────────┐
      │     CrossPlatformAnalytics        │
      │  crossplatform_analytics.duckdb   │
      │                                   │
      │  - unified_video_daily (fact)     │
      │  - unified_webcasts (fact)        │
      │  - dim_accounts (dimension)       │
      └───────────────────────────────────┘
```

## Quick Start

### 1. Prerequisites

Ensure you have data in at least one source database:
- **Vbrick**: Run `01_fetch_analytics.py` and `02_Webcast.py` in the Vbrick folder
- **Brightcove**: Run `3_daily_analytics.py` in the UnifiedPipeline folder

### 2. Sync Data

```bash
# Sync all available data
python sync_all.py

# Or sync individual platforms
python sync_vbrick.py
python sync_brightcove.py

# Preview without writing
python sync_all.py --dry-run
```

### 3. Run Queries

```bash
# Run default queries
python query_examples.py

# Run all example queries
python query_examples.py --all

# Run specific query
python query_examples.py --query views_by_platform

# List available queries
python query_examples.py --list
```

### 4. Export to Parquet (for PowerBI)

```bash
# Export all tables to Parquet
python to_parquet.py

# Export with per-platform fact files
python to_parquet.py --by-platform

# Show statistics only
python to_parquet.py --stats
```

## Database Schema

### unified_video_daily (Fact Table)

Primary Key: `(platform, account_id, video_id, date)`

| Column | Type | Description |
|--------|------|-------------|
| platform | VARCHAR | 'vbrick' or 'brightcove' |
| account_id | VARCHAR | Account ID (or 'vbrick' for Vbrick) |
| video_id | VARCHAR | Video identifier |
| date | DATE | Date of analytics |
| views | INTEGER | Total views |
| views_desktop | INTEGER | Desktop views |
| views_mobile | INTEGER | Mobile views |
| views_tablet | INTEGER | Tablet views (Brightcove only) |
| browser_chrome | INTEGER | Chrome views (Vbrick only) |
| engagement_score | DOUBLE | Engagement score (Brightcove only) |
| ... | ... | Additional platform-specific fields |

### unified_webcasts (Fact Table - Vbrick only)

Primary Key: `event_id`

| Column | Type | Description |
|--------|------|-------------|
| event_id | VARCHAR | Webcast event ID |
| title | VARCHAR | Event title |
| attendee_total | INTEGER | Total attendance |
| zone_apac | INTEGER | APAC attendees |
| zone_americas | INTEGER | Americas attendees |
| zone_emea | INTEGER | EMEA attendees |
| ... | ... | Additional fields |

### dim_accounts (Dimension Table)

Primary Key: `(platform, account_id)`

| Column | Type | Description |
|--------|------|-------------|
| platform | VARCHAR | 'vbrick' or 'brightcove' |
| account_id | VARCHAR | Account identifier |
| account_name | VARCHAR | Human-readable name |
| account_category | VARCHAR | Category grouping |

## Example Queries

### Total Views by Platform

```sql
SELECT
    platform,
    SUM(views) as total_views,
    COUNT(DISTINCT video_id) as unique_videos
FROM unified_video_daily
GROUP BY platform;
```

### Device Breakdown Comparison

```sql
SELECT
    platform,
    ROUND(SUM(views_desktop) * 100.0 / SUM(views), 1) as pct_desktop,
    ROUND(SUM(views_mobile) * 100.0 / SUM(views), 1) as pct_mobile
FROM unified_video_daily
WHERE views > 0
GROUP BY platform;
```

### Monthly Trends

```sql
SELECT
    DATE_TRUNC('month', date) as month,
    platform,
    SUM(views) as total_views
FROM unified_video_daily
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Webcast VOD Performance

```sql
SELECT
    w.title,
    w.attendee_total as live_attendance,
    SUM(v.views) as vod_views
FROM unified_webcasts w
LEFT JOIN unified_video_daily v
    ON w.vod_video_id = v.video_id
    AND v.platform = 'vbrick'
GROUP BY w.event_id, w.title, w.attendee_total;
```

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `shared_crossplatform.py` | Core utilities, schema, upsert functions |
| `sync_vbrick.py` | Sync Vbrick data to unified DB |
| `sync_brightcove.py` | Sync Brightcove data to unified DB |
| `sync_all.py` | Master sync orchestrator |
| `query_examples.py` | Example cross-platform queries |
| `to_parquet.py` | Export to Parquet for PowerBI |

## Command Line Options

### sync_all.py

```bash
python sync_all.py                # Sync all data
python sync_all.py --stats        # Show statistics only
python sync_all.py --dry-run      # Preview without writing
python sync_all.py --vbrick-only  # Sync Vbrick only
python sync_all.py --brightcove-only  # Sync Brightcove only
```

### query_examples.py

```bash
python query_examples.py          # Run default queries
python query_examples.py --all    # Run all queries
python query_examples.py --query <name>  # Run specific query
python query_examples.py --list   # List available queries
```

## Data Flow

1. **Source pipelines** (Vbrick, UnifiedPipeline) fetch data from APIs and store in separate DuckDB databases
2. **Sync scripts** read from source databases, transform column names, and upsert to unified database
3. **Unified database** enables cross-platform queries and reporting

## Column Mapping

### Vbrick to Unified

| Vbrick Column | Unified Column |
|---------------|----------------|
| views | views |
| device_desktop | views_desktop |
| device_mobile | views_mobile |
| device_other | views_other |
| browser_chrome | browser_chrome |
| browser_edge | browser_edge |
| duration | duration_seconds |
| title | title |

### Brightcove to Unified

| Brightcove Column | Unified Column |
|-------------------|----------------|
| video_view | views |
| views_desktop | views_desktop |
| views_mobile | views_mobile |
| views_tablet | views_tablet |
| name | title |
| video_duration | duration_seconds |
| created_by | uploaded_by |

## Troubleshooting

**Database not found**
```
Run the source pipelines first:
- Vbrick: python 01_fetch_analytics.py
- Brightcove: python 3_daily_analytics.py
```

**No data after sync**
```
Check source database exists and contains data:
python sync_all.py --stats
```

**Query errors**
```
Ensure database is populated:
python query_examples.py --query date_coverage
```

## PowerBI / Parquet Export

The `to_parquet.py` script exports data in a star schema format optimized for PowerBI:

### Star Schema Design

```
┌──────────────────────┐       ┌───────────────────────┐
│    dim_videos        │       │    dim_accounts       │
│  (one row per video) │       │  (platform/account)   │
└──────────┬───────────┘       └───────────┬───────────┘
           │                               │
           │  video_id                     │  account_id
           │                               │
           ▼                               ▼
┌────────────────────────────────────────────────────────┐
│              daily_video_facts                         │
│         (metrics per video per day)                    │
│                                                        │
│  - Avoids SUM(duration) issues                         │
│  - Enables proper aggregation in PowerBI               │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│              webcast_facts                             │
│         (Vbrick webcast attendance)                    │
└────────────────────────────────────────────────────────┘
```

### Parquet Output Structure

```
output/parquet/
├── dimensions/
│   ├── dim_videos.parquet        # Video metadata
│   └── dim_accounts.parquet      # Account info
└── facts/
    ├── daily_video_facts.parquet           # All platforms
    ├── daily_video_facts_vbrick.parquet    # (--by-platform)
    ├── daily_video_facts_brightcove.parquet
    └── webcast_facts.parquet               # Webcasts
```

### to_parquet.py Options

```bash
python to_parquet.py              # Export all tables
python to_parquet.py --stats      # Show statistics
python to_parquet.py --facts-only # Skip dimensions
python to_parquet.py --by-platform # Create per-platform files
```

## Output Location

```
CrossPlatformAnalytics/
└── output/
    ├── crossplatform_analytics.duckdb
    └── parquet/
        ├── dimensions/
        │   ├── dim_videos.parquet
        │   └── dim_accounts.parquet
        └── facts/
            ├── daily_video_facts.parquet
            └── webcast_facts.parquet
```
