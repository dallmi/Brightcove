"""
shared_crossplatform.py - Shared utilities for CrossPlatformAnalytics

Contains:
- Database schema and initialization
- Upsert functions for unified tables
- Column mapping utilities
- Path utilities
"""

import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# =============================================================================
# PATH UTILITIES
# =============================================================================

def get_project_root() -> Path:
    """Get the CrossPlatformAnalytics project root directory."""
    return Path(__file__).parent


def get_output_dir() -> Path:
    """Get the output directory for generated files."""
    output_dir = get_project_root() / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def get_crossplatform_db_path() -> Path:
    """Get path to crossplatform_analytics.duckdb."""
    return get_output_dir() / "crossplatform_analytics.duckdb"


def get_vbrick_db_path() -> Path:
    """Get path to Vbrick source database."""
    return get_project_root().parent / "Vbrick" / "output" / "vbrick_analytics.duckdb"


def get_brightcove_db_path() -> Path:
    """Get path to Brightcove/UnifiedPipeline source database."""
    return get_project_root().parent / "UnifiedPipeline" / "output" / "analytics.duckdb"


# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

def init_crossplatform_db(db_path: Optional[Path] = None) -> 'duckdb.DuckDBPyConnection':
    """
    Initialize the cross-platform DuckDB database with unified schema.

    Args:
        db_path: Optional path to database file. Defaults to output/crossplatform_analytics.duckdb

    Returns:
        DuckDB connection
    """
    import duckdb

    if db_path is None:
        db_path = get_crossplatform_db_path()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # Create unified_video_daily table (fact)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS unified_video_daily (
            -- PRIMARY KEY
            platform VARCHAR NOT NULL,
            account_id VARCHAR NOT NULL,
            video_id VARCHAR NOT NULL,
            date DATE NOT NULL,

            -- IDENTIFIERS
            channel VARCHAR,
            title VARCHAR,

            -- VIEW METRICS (standardized)
            views INTEGER DEFAULT 0,
            views_desktop INTEGER DEFAULT 0,
            views_mobile INTEGER DEFAULT 0,
            views_tablet INTEGER DEFAULT 0,
            views_other INTEGER DEFAULT 0,

            -- BROWSER BREAKDOWN (Vbrick only)
            browser_chrome INTEGER DEFAULT 0,
            browser_edge INTEGER DEFAULT 0,
            browser_other INTEGER DEFAULT 0,

            -- ENGAGEMENT METRICS (Brightcove only)
            video_impression INTEGER DEFAULT 0,
            play_rate DOUBLE DEFAULT 0,
            engagement_score DOUBLE DEFAULT 0,
            video_engagement_1 DOUBLE DEFAULT 0,
            video_engagement_25 DOUBLE DEFAULT 0,
            video_engagement_50 DOUBLE DEFAULT 0,
            video_engagement_75 DOUBLE DEFAULT 0,
            video_engagement_100 DOUBLE DEFAULT 0,
            video_percent_viewed DOUBLE DEFAULT 0,
            video_seconds_viewed INTEGER DEFAULT 0,

            -- VIDEO METADATA (denormalized)
            duration_seconds INTEGER,
            created_at VARCHAR,
            published_at VARCHAR,
            uploaded_by VARCHAR,
            tags VARCHAR,
            category VARCHAR,

            -- PLATFORM-SPECIFIC METADATA (Brightcove)
            reference_id VARCHAR,
            dt_last_viewed VARCHAR,
            video_content_type VARCHAR,
            business_unit VARCHAR,
            cf_video_owner_email VARCHAR,

            -- PLATFORM-SPECIFIC METADATA (Vbrick)
            playback_url VARCHAR,
            comment_count INTEGER,
            score DOUBLE,

            -- META
            report_generated_on VARCHAR,

            PRIMARY KEY (platform, account_id, video_id, date)
        )
    """)

    # Create unified_webcasts table (Vbrick only)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS unified_webcasts (
            event_id VARCHAR NOT NULL PRIMARY KEY,
            platform VARCHAR NOT NULL DEFAULT 'vbrick',
            title VARCHAR,
            event_url VARCHAR,
            vod_video_id VARCHAR,
            start_date VARCHAR,
            end_date VARCHAR,

            -- ATTENDANCE
            attendee_count INTEGER DEFAULT 0,
            attendee_total INTEGER DEFAULT 0,
            total_viewing_time INTEGER DEFAULT 0,

            -- ZONES
            zone_apac INTEGER DEFAULT 0,
            zone_americas INTEGER DEFAULT 0,
            zone_emea INTEGER DEFAULT 0,
            zone_swiss INTEGER DEFAULT 0,
            zone_other INTEGER DEFAULT 0,

            -- DEVICE/BROWSER
            browser_chrome INTEGER DEFAULT 0,
            browser_edge INTEGER DEFAULT 0,
            browser_other INTEGER DEFAULT 0,
            device_pc INTEGER DEFAULT 0,
            device_mobile INTEGER DEFAULT 0,
            device_other INTEGER DEFAULT 0,

            -- CATEGORIZATION
            category VARCHAR,
            subcategory VARCHAR,

            report_generated_on VARCHAR
        )
    """)

    # Create dim_accounts table (dimension)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_accounts (
            platform VARCHAR NOT NULL,
            account_id VARCHAR NOT NULL,
            account_name VARCHAR,
            account_category VARCHAR,
            is_active BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (platform, account_id)
        )
    """)

    # Create indexes for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_uvd_date
        ON unified_video_daily (date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_uvd_platform_date
        ON unified_video_daily (platform, date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_uvd_video_date
        ON unified_video_daily (video_id, date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_uw_start_date
        ON unified_webcasts (start_date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_uw_vod_video
        ON unified_webcasts (vod_video_id)
    """)

    return conn


# =============================================================================
# UPSERT FUNCTIONS
# =============================================================================

def upsert_video_daily(
    conn: 'duckdb.DuckDBPyConnection',
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Upsert rows into unified_video_daily table.

    Args:
        conn: DuckDB connection
        rows: List of row dicts with unified column names
        logger: Optional logger

    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0

    if logger is None:
        logger = logging.getLogger('CrossPlatform')

    columns = [
        'platform', 'account_id', 'video_id', 'date',
        'channel', 'title',
        'views', 'views_desktop', 'views_mobile', 'views_tablet', 'views_other',
        'browser_chrome', 'browser_edge', 'browser_other',
        'video_impression', 'play_rate', 'engagement_score',
        'video_engagement_1', 'video_engagement_25', 'video_engagement_50',
        'video_engagement_75', 'video_engagement_100',
        'video_percent_viewed', 'video_seconds_viewed',
        'duration_seconds', 'created_at', 'published_at', 'uploaded_by',
        'tags', 'category',
        'reference_id', 'dt_last_viewed', 'video_content_type',
        'business_unit', 'cf_video_owner_email',
        'playback_url', 'comment_count', 'score',
        'report_generated_on'
    ]

    placeholders = ', '.join(['?' for _ in columns])
    column_list = ', '.join(columns)

    query = f"""
        INSERT OR REPLACE INTO unified_video_daily ({column_list})
        VALUES ({placeholders})
    """

    values = []
    for row in rows:
        row_values = tuple(row.get(col) for col in columns)
        values.append(row_values)

    conn.executemany(query, values)

    logger.debug(f"Upserted {len(rows)} rows to unified_video_daily")
    return len(rows)


def upsert_webcasts(
    conn: 'duckdb.DuckDBPyConnection',
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Upsert rows into unified_webcasts table.

    Args:
        conn: DuckDB connection
        rows: List of row dicts with unified column names
        logger: Optional logger

    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0

    if logger is None:
        logger = logging.getLogger('CrossPlatform')

    columns = [
        'event_id', 'platform', 'title', 'event_url', 'vod_video_id',
        'start_date', 'end_date',
        'attendee_count', 'attendee_total', 'total_viewing_time',
        'zone_apac', 'zone_americas', 'zone_emea', 'zone_swiss', 'zone_other',
        'browser_chrome', 'browser_edge', 'browser_other',
        'device_pc', 'device_mobile', 'device_other',
        'category', 'subcategory', 'report_generated_on'
    ]

    placeholders = ', '.join(['?' for _ in columns])
    column_list = ', '.join(columns)

    query = f"""
        INSERT OR REPLACE INTO unified_webcasts ({column_list})
        VALUES ({placeholders})
    """

    values = []
    for row in rows:
        row_values = tuple(row.get(col) for col in columns)
        values.append(row_values)

    conn.executemany(query, values)

    logger.debug(f"Upserted {len(rows)} rows to unified_webcasts")
    return len(rows)


def upsert_accounts(
    conn: 'duckdb.DuckDBPyConnection',
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Upsert rows into dim_accounts table.

    Args:
        conn: DuckDB connection
        rows: List of row dicts
        logger: Optional logger

    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0

    if logger is None:
        logger = logging.getLogger('CrossPlatform')

    columns = ['platform', 'account_id', 'account_name', 'account_category', 'is_active']
    placeholders = ', '.join(['?' for _ in columns])
    column_list = ', '.join(columns)

    query = f"""
        INSERT OR REPLACE INTO dim_accounts ({column_list})
        VALUES ({placeholders})
    """

    values = []
    for row in rows:
        row_values = tuple(row.get(col) for col in columns)
        values.append(row_values)

    conn.executemany(query, values)

    logger.debug(f"Upserted {len(rows)} rows to dim_accounts")
    return len(rows)


# =============================================================================
# COLUMN MAPPING - VBRICK
# =============================================================================

def map_vbrick_video_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a Vbrick vbrick_video_daily row to unified schema.

    Args:
        row: Dict from vbrick_video_daily table

    Returns:
        Dict with unified column names
    """
    return {
        'platform': 'vbrick',
        'account_id': 'vbrick',
        'video_id': row.get('video_id'),
        'date': row.get('date'),
        'channel': None,
        'title': row.get('title'),
        'views': row.get('views', 0),
        'views_desktop': row.get('device_desktop', 0),
        'views_mobile': row.get('device_mobile', 0),
        'views_tablet': None,  # Vbrick doesn't track tablet separately
        'views_other': row.get('device_other', 0),
        'browser_chrome': row.get('browser_chrome', 0),
        'browser_edge': row.get('browser_edge', 0),
        'browser_other': row.get('browser_other', 0),
        # Engagement metrics not available in Vbrick
        'video_impression': None,
        'play_rate': None,
        'engagement_score': None,
        'video_engagement_1': None,
        'video_engagement_25': None,
        'video_engagement_50': None,
        'video_engagement_75': None,
        'video_engagement_100': None,
        'video_percent_viewed': None,
        'video_seconds_viewed': None,
        # Metadata
        'duration_seconds': row.get('duration'),
        'created_at': row.get('when_uploaded'),
        'published_at': row.get('when_published'),
        'uploaded_by': row.get('uploaded_by'),
        'tags': row.get('tags'),
        'category': None,  # Vbrick videos don't have category in video table
        # Brightcove-specific (null for Vbrick)
        'reference_id': None,
        'dt_last_viewed': None,
        'video_content_type': None,
        'business_unit': None,
        'cf_video_owner_email': None,
        # Vbrick-specific
        'playback_url': row.get('playback_url'),
        'comment_count': row.get('comment_count'),
        'score': row.get('score'),
        'report_generated_on': row.get('report_generated_on'),
    }


def map_vbrick_webcast_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a Vbrick vbrick_webcasts row to unified schema.

    Args:
        row: Dict from vbrick_webcasts table

    Returns:
        Dict with unified column names
    """
    return {
        'event_id': row.get('event_id'),
        'platform': 'vbrick',
        'title': row.get('title'),
        'event_url': row.get('event_url'),
        'vod_video_id': row.get('vod_id'),
        'start_date': row.get('start_date'),
        'end_date': row.get('end_date'),
        'attendee_count': row.get('attendee_count', 0),
        'attendee_total': row.get('attendee_total', 0),
        'total_viewing_time': row.get('total_viewing_time', 0),
        'zone_apac': row.get('zone_apac', 0),
        'zone_americas': row.get('zone_america', 0),  # Note: source is 'zone_america' (singular)
        'zone_emea': row.get('zone_emea', 0),
        'zone_swiss': row.get('zone_swiss', 0),
        'zone_other': row.get('zone_other', 0),
        'browser_chrome': row.get('browser_chrome', 0),
        'browser_edge': row.get('browser_edge', 0),
        'browser_other': row.get('browser_other', 0),
        'device_pc': row.get('device_pc', 0),
        'device_mobile': row.get('device_mobile', 0),
        'device_other': row.get('device_other', 0),
        'category': row.get('category'),
        'subcategory': row.get('subcategory'),
        'report_generated_on': row.get('report_generated_on'),
    }


# =============================================================================
# COLUMN MAPPING - BRIGHTCOVE
# =============================================================================

def map_brightcove_video_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a Brightcove daily_analytics row to unified schema.

    Args:
        row: Dict from daily_analytics table

    Returns:
        Dict with unified column names
    """
    return {
        'platform': 'brightcove',
        'account_id': row.get('account_id'),
        'video_id': row.get('video_id'),
        'date': row.get('date'),
        'channel': row.get('channel'),
        'title': row.get('name'),  # Brightcove uses 'name' for title
        'views': row.get('video_view', 0),
        'views_desktop': row.get('views_desktop', 0),
        'views_mobile': row.get('views_mobile', 0),
        'views_tablet': row.get('views_tablet', 0),
        'views_other': row.get('views_other', 0),
        # Browser metrics not available in Brightcove
        'browser_chrome': None,
        'browser_edge': None,
        'browser_other': None,
        # Engagement metrics
        'video_impression': row.get('video_impression', 0),
        'play_rate': row.get('play_rate', 0),
        'engagement_score': row.get('engagement_score', 0),
        'video_engagement_1': row.get('video_engagement_1', 0),
        'video_engagement_25': row.get('video_engagement_25', 0),
        'video_engagement_50': row.get('video_engagement_50', 0),
        'video_engagement_75': row.get('video_engagement_75', 0),
        'video_engagement_100': row.get('video_engagement_100', 0),
        'video_percent_viewed': row.get('video_percent_viewed', 0),
        'video_seconds_viewed': row.get('video_seconds_viewed', 0),
        # Metadata
        'duration_seconds': row.get('video_duration'),
        'created_at': row.get('created_at'),
        'published_at': row.get('published_at'),
        'uploaded_by': row.get('created_by'),
        'tags': row.get('tags'),
        'category': row.get('video_category'),
        # Brightcove-specific
        'reference_id': row.get('reference_id'),
        'dt_last_viewed': row.get('dt_last_viewed'),
        'video_content_type': row.get('video_content_type'),
        'business_unit': row.get('business_unit'),
        'cf_video_owner_email': row.get('cf_video_owner_email'),
        # Vbrick-specific (null for Brightcove)
        'playback_url': None,
        'comment_count': None,
        'score': None,
        'report_generated_on': row.get('report_generated_on'),
    }


# =============================================================================
# STATISTICS
# =============================================================================

def get_db_stats(conn: 'duckdb.DuckDBPyConnection') -> Dict[str, Any]:
    """
    Get statistics about the cross-platform database.

    Args:
        conn: DuckDB connection

    Returns:
        Dict with statistics
    """
    stats = {}

    # Video daily stats by platform
    result = conn.execute("""
        SELECT
            platform,
            COUNT(*) as row_count,
            COUNT(DISTINCT video_id) as video_count,
            MIN(date) as min_date,
            MAX(date) as max_date,
            SUM(views) as total_views
        FROM unified_video_daily
        GROUP BY platform
    """).fetchall()

    stats['video_daily_by_platform'] = [
        {
            'platform': row[0],
            'row_count': row[1],
            'video_count': row[2],
            'min_date': str(row[3]) if row[3] else None,
            'max_date': str(row[4]) if row[4] else None,
            'total_views': row[5]
        }
        for row in result
    ]

    # Webcast stats
    result = conn.execute("""
        SELECT
            COUNT(*) as event_count,
            MIN(start_date) as min_date,
            MAX(start_date) as max_date,
            SUM(attendee_total) as total_attendance
        FROM unified_webcasts
    """).fetchone()

    stats['webcasts'] = {
        'event_count': result[0],
        'min_date': result[1],
        'max_date': result[2],
        'total_attendance': result[3]
    }

    # Account stats
    result = conn.execute("""
        SELECT platform, COUNT(*) as account_count
        FROM dim_accounts
        GROUP BY platform
    """).fetchall()

    stats['accounts_by_platform'] = {row[0]: row[1] for row in result}

    return stats


def print_db_stats(conn: Optional['duckdb.DuckDBPyConnection'] = None, logger: Optional[logging.Logger] = None):
    """
    Print database statistics.

    Args:
        conn: Optional DuckDB connection. Opens one if not provided.
        logger: Optional logger
    """
    if logger is None:
        logger = logging.getLogger('CrossPlatform')

    close_conn = False
    if conn is None:
        conn = init_crossplatform_db()
        close_conn = True

    stats = get_db_stats(conn)

    logger.info("=" * 60)
    logger.info("Cross-Platform Analytics Database Statistics")
    logger.info("=" * 60)

    logger.info("\nVideo Daily Analytics:")
    for platform_stats in stats['video_daily_by_platform']:
        logger.info(f"  {platform_stats['platform'].upper()}:")
        logger.info(f"    Rows: {platform_stats['row_count']:,}")
        logger.info(f"    Videos: {platform_stats['video_count']:,}")
        logger.info(f"    Date range: {platform_stats['min_date']} to {platform_stats['max_date']}")
        logger.info(f"    Total views: {platform_stats['total_views']:,}")

    logger.info("\nWebcasts (Vbrick only):")
    wc = stats['webcasts']
    logger.info(f"  Events: {wc['event_count']:,}")
    logger.info(f"  Date range: {wc['min_date']} to {wc['max_date']}")
    logger.info(f"  Total attendance: {wc['total_attendance']:,}")

    logger.info("\nAccounts:")
    for platform, count in stats['accounts_by_platform'].items():
        logger.info(f"  {platform}: {count}")

    logger.info("=" * 60)

    if close_conn:
        conn.close()
