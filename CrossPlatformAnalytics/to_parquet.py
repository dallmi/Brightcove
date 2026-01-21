"""
to_parquet.py - Export CrossPlatformAnalytics to Parquet for PowerBI

This script exports the unified DuckDB database to Parquet format,
following a star schema design optimized for PowerBI reporting.

Star Schema Design:
    - Fact: daily_video_facts (metrics per video per day)
    - Fact: webcast_facts (webcast attendance data)
    - Dimension: dim_videos (one row per video with metadata)
    - Dimension: dim_accounts (platform/account info)

Output:
    output/parquet/
    ├── dimensions/
    │   ├── dim_videos.parquet        # Video metadata (one row per video)
    │   └── dim_accounts.parquet      # Account information
    └── facts/
        ├── daily_video_facts.parquet # Daily video analytics
        ├── daily_video_facts_vbrick.parquet
        ├── daily_video_facts_brightcove.parquet
        └── webcast_facts.parquet     # Webcast attendance

Usage:
    python to_parquet.py              # Export all tables
    python to_parquet.py --stats      # Show statistics only
    python to_parquet.py --facts-only # Export facts only
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

from shared_crossplatform import (
    init_crossplatform_db,
    get_crossplatform_db_path,
    get_output_dir,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_parquet_dir() -> Path:
    """Get the Parquet output directory."""
    parquet_dir = get_output_dir() / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    return parquet_dir


def export_dim_videos(conn, output_dir: Path) -> int:
    """
    Export video dimension table (one row per unique video).

    This prevents SUM(duration) issues when aggregating in PowerBI.

    Args:
        conn: DuckDB connection
        output_dir: Output directory for Parquet files

    Returns:
        Number of rows exported
    """
    logger.info("Exporting dim_videos...")

    # Get one row per video with latest metadata
    query = """
        SELECT
            platform,
            account_id,
            video_id,
            FIRST(title) as title,
            FIRST(channel) as channel,
            FIRST(duration_seconds) as duration_seconds,
            FIRST(created_at) as created_at,
            FIRST(published_at) as published_at,
            FIRST(uploaded_by) as uploaded_by,
            FIRST(tags) as tags,
            FIRST(category) as category,
            -- Brightcove specific
            FIRST(reference_id) as reference_id,
            FIRST(video_content_type) as video_content_type,
            FIRST(business_unit) as business_unit,
            FIRST(cf_video_owner_email) as cf_video_owner_email,
            -- Vbrick specific
            FIRST(playback_url) as playback_url,
            -- Aggregated stats
            MIN(date) as first_view_date,
            MAX(date) as last_view_date,
            SUM(views) as total_views
        FROM unified_video_daily
        GROUP BY platform, account_id, video_id
        ORDER BY platform, account_id, video_id
    """

    df = conn.execute(query).fetchdf()

    if df.empty:
        logger.warning("No video data to export")
        return 0

    # Convert date columns
    for col in ['first_view_date', 'last_view_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Export
    dimensions_dir = output_dir / "dimensions"
    dimensions_dir.mkdir(parents=True, exist_ok=True)

    output_path = dimensions_dir / "dim_videos.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')

    logger.info(f"Exported {len(df)} videos to {output_path}")
    return len(df)


def export_dim_accounts(conn, output_dir: Path) -> int:
    """
    Export account dimension table.

    Args:
        conn: DuckDB connection
        output_dir: Output directory for Parquet files

    Returns:
        Number of rows exported
    """
    logger.info("Exporting dim_accounts...")

    query = """
        SELECT
            platform,
            account_id,
            account_name,
            account_category,
            is_active
        FROM dim_accounts
        ORDER BY platform, account_name
    """

    df = conn.execute(query).fetchdf()

    if df.empty:
        logger.warning("No account data to export")
        return 0

    # Export
    dimensions_dir = output_dir / "dimensions"
    dimensions_dir.mkdir(parents=True, exist_ok=True)

    output_path = dimensions_dir / "dim_accounts.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')

    logger.info(f"Exported {len(df)} accounts to {output_path}")
    return len(df)


def export_daily_video_facts(conn, output_dir: Path, platform: str = None) -> int:
    """
    Export daily video facts table.

    Args:
        conn: DuckDB connection
        output_dir: Output directory for Parquet files
        platform: Optional platform filter ('vbrick' or 'brightcove')

    Returns:
        Number of rows exported
    """
    platform_filter = f"WHERE platform = '{platform}'" if platform else ""
    platform_suffix = f"_{platform}" if platform else ""

    logger.info(f"Exporting daily_video_facts{platform_suffix}...")

    # Fact table with metrics only (join to dim_videos for metadata)
    query = f"""
        SELECT
            platform,
            account_id,
            video_id,
            date,
            EXTRACT(YEAR FROM date) as year,
            channel,
            title,
            -- View metrics
            views,
            views_desktop,
            views_mobile,
            views_tablet,
            views_other,
            -- Browser metrics (Vbrick)
            browser_chrome,
            browser_edge,
            browser_other,
            -- Engagement metrics (Brightcove)
            video_impression,
            play_rate,
            engagement_score,
            video_engagement_1,
            video_engagement_25,
            video_engagement_50,
            video_engagement_75,
            video_engagement_100,
            video_percent_viewed,
            video_seconds_viewed,
            -- Meta
            report_generated_on
        FROM unified_video_daily
        {platform_filter}
        ORDER BY platform, account_id, video_id, date
    """

    df = conn.execute(query).fetchdf()

    if df.empty:
        logger.warning(f"No video facts to export{platform_suffix}")
        return 0

    # Convert date column
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Convert integer columns
    int_cols = ['views', 'views_desktop', 'views_mobile', 'views_tablet', 'views_other',
                'browser_chrome', 'browser_edge', 'browser_other',
                'video_impression', 'video_seconds_viewed', 'year']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Convert float columns
    float_cols = ['play_rate', 'engagement_score', 'video_engagement_1', 'video_engagement_25',
                  'video_engagement_50', 'video_engagement_75', 'video_engagement_100',
                  'video_percent_viewed']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Export
    facts_dir = output_dir / "facts"
    facts_dir.mkdir(parents=True, exist_ok=True)

    output_path = facts_dir / f"daily_video_facts{platform_suffix}.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')

    logger.info(f"Exported {len(df)} rows to {output_path}")
    return len(df)


def export_webcast_facts(conn, output_dir: Path) -> int:
    """
    Export webcast facts table.

    Args:
        conn: DuckDB connection
        output_dir: Output directory for Parquet files

    Returns:
        Number of rows exported
    """
    logger.info("Exporting webcast_facts...")

    query = """
        SELECT
            event_id,
            platform,
            title,
            event_url,
            vod_video_id,
            start_date,
            end_date,
            EXTRACT(YEAR FROM CAST(start_date AS DATE)) as year,
            -- Attendance
            attendee_count,
            attendee_total,
            total_viewing_time,
            -- Zones
            zone_apac,
            zone_americas,
            zone_emea,
            zone_swiss,
            zone_other,
            -- Device/Browser
            browser_chrome,
            browser_edge,
            browser_other,
            device_pc,
            device_mobile,
            device_other,
            -- Categorization
            category,
            subcategory,
            report_generated_on
        FROM unified_webcasts
        ORDER BY start_date, event_id
    """

    df = conn.execute(query).fetchdf()

    if df.empty:
        logger.warning("No webcast facts to export")
        return 0

    # Convert date columns
    for col in ['start_date', 'end_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Convert integer columns
    int_cols = ['attendee_count', 'attendee_total', 'total_viewing_time', 'year',
                'zone_apac', 'zone_americas', 'zone_emea', 'zone_swiss', 'zone_other',
                'browser_chrome', 'browser_edge', 'browser_other',
                'device_pc', 'device_mobile', 'device_other']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # Export
    facts_dir = output_dir / "facts"
    facts_dir.mkdir(parents=True, exist_ok=True)

    output_path = facts_dir / "webcast_facts.parquet"
    df.to_parquet(output_path, index=False, compression='snappy')

    logger.info(f"Exported {len(df)} webcasts to {output_path}")
    return len(df)


def show_stats(conn):
    """Show database statistics."""
    logger.info("\n" + "=" * 60)
    logger.info("Database Statistics")
    logger.info("=" * 60)

    # Video facts by platform
    result = conn.execute("""
        SELECT
            platform,
            COUNT(*) as rows,
            COUNT(DISTINCT video_id) as videos,
            SUM(views) as total_views
        FROM unified_video_daily
        GROUP BY platform
    """).fetchall()

    logger.info("\nVideo Facts:")
    for row in result:
        logger.info(f"  {row[0]}: {row[1]:,} rows, {row[2]:,} videos, {row[3]:,} views")

    # Webcasts
    result = conn.execute("""
        SELECT COUNT(*), SUM(attendee_total) FROM unified_webcasts
    """).fetchone()
    logger.info(f"\nWebcasts: {result[0]:,} events, {result[1]:,} total attendance")

    # Accounts
    result = conn.execute("""
        SELECT platform, COUNT(*) FROM dim_accounts GROUP BY platform
    """).fetchall()
    logger.info("\nAccounts:")
    for row in result:
        logger.info(f"  {row[0]}: {row[1]}")


def main():
    parser = argparse.ArgumentParser(description='Export CrossPlatformAnalytics to Parquet')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--facts-only', action='store_true', help='Export facts only (skip dimensions)')
    parser.add_argument('--by-platform', action='store_true', help='Also export per-platform fact files')
    args = parser.parse_args()

    # Check database exists
    db_path = get_crossplatform_db_path()
    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        logger.error("Run sync_all.py first to populate the database")
        return

    conn = init_crossplatform_db()
    parquet_dir = get_parquet_dir()

    if args.stats:
        show_stats(conn)
        conn.close()
        return

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Exporting to Parquet for PowerBI")
    logger.info("=" * 60)

    stats = {
        'dim_videos': 0,
        'dim_accounts': 0,
        'video_facts': 0,
        'video_facts_vbrick': 0,
        'video_facts_brightcove': 0,
        'webcast_facts': 0
    }

    try:
        # Export dimensions
        if not args.facts_only:
            stats['dim_videos'] = export_dim_videos(conn, parquet_dir)
            stats['dim_accounts'] = export_dim_accounts(conn, parquet_dir)

        # Export facts
        stats['video_facts'] = export_daily_video_facts(conn, parquet_dir)
        stats['webcast_facts'] = export_webcast_facts(conn, parquet_dir)

        # Export per-platform facts if requested
        if args.by_platform:
            stats['video_facts_vbrick'] = export_daily_video_facts(conn, parquet_dir, 'vbrick')
            stats['video_facts_brightcove'] = export_daily_video_facts(conn, parquet_dir, 'brightcove')

    finally:
        conn.close()

    # Summary
    elapsed = datetime.now() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("EXPORT COMPLETE")
    logger.info("=" * 60)

    if not args.facts_only:
        logger.info(f"\nDimensions:")
        logger.info(f"  dim_videos: {stats['dim_videos']:,} rows")
        logger.info(f"  dim_accounts: {stats['dim_accounts']:,} rows")

    logger.info(f"\nFacts:")
    logger.info(f"  daily_video_facts: {stats['video_facts']:,} rows")
    if args.by_platform:
        logger.info(f"  daily_video_facts_vbrick: {stats['video_facts_vbrick']:,} rows")
        logger.info(f"  daily_video_facts_brightcove: {stats['video_facts_brightcove']:,} rows")
    logger.info(f"  webcast_facts: {stats['webcast_facts']:,} rows")

    logger.info(f"\nElapsed time: {elapsed}")
    logger.info(f"Output directory: {parquet_dir}")

    # List output files
    logger.info("\nOutput files:")
    for path in sorted(parquet_dir.rglob("*.parquet")):
        size_mb = path.stat().st_size / (1024 * 1024)
        logger.info(f"  {path.relative_to(parquet_dir)}: {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
