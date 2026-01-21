"""
query_examples.py - Example cross-platform queries

This script demonstrates various queries you can run against the
unified cross-platform analytics database.

Usage:
    python query_examples.py                    # Run all example queries
    python query_examples.py --query views      # Run specific query
    python query_examples.py --list             # List available queries
"""

import argparse
import logging
from shared_crossplatform import init_crossplatform_db, get_crossplatform_db_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# =============================================================================
# QUERY DEFINITIONS
# =============================================================================

QUERIES = {
    'views_by_platform': {
        'name': 'Total Views by Platform',
        'description': 'Compare total views between Vbrick and Brightcove',
        'sql': """
            SELECT
                platform,
                SUM(views) as total_views,
                COUNT(DISTINCT video_id) as unique_videos,
                COUNT(*) as data_points
            FROM unified_video_daily
            GROUP BY platform
            ORDER BY total_views DESC
        """
    },

    'device_breakdown': {
        'name': 'Device Breakdown by Platform',
        'description': 'Compare device usage between platforms (percentage)',
        'sql': """
            SELECT
                platform,
                SUM(views) as total_views,
                ROUND(SUM(views_desktop) * 100.0 / NULLIF(SUM(views), 0), 1) as pct_desktop,
                ROUND(SUM(views_mobile) * 100.0 / NULLIF(SUM(views), 0), 1) as pct_mobile,
                ROUND(SUM(views_tablet) * 100.0 / NULLIF(SUM(views), 0), 1) as pct_tablet,
                ROUND(SUM(views_other) * 100.0 / NULLIF(SUM(views), 0), 1) as pct_other
            FROM unified_video_daily
            WHERE views > 0
            GROUP BY platform
        """
    },

    'monthly_trend': {
        'name': 'Monthly Trend by Platform',
        'description': 'View trends over time by platform',
        'sql': """
            SELECT
                DATE_TRUNC('month', date) as month,
                platform,
                SUM(views) as total_views,
                COUNT(DISTINCT video_id) as active_videos
            FROM unified_video_daily
            GROUP BY DATE_TRUNC('month', date), platform
            ORDER BY month DESC, platform
            LIMIT 24
        """
    },

    'top_videos': {
        'name': 'Top 10 Videos by Platform',
        'description': 'Most viewed videos on each platform',
        'sql': """
            WITH video_totals AS (
                SELECT
                    platform,
                    video_id,
                    title,
                    SUM(views) as total_views,
                    MAX(date) as last_viewed
                FROM unified_video_daily
                GROUP BY platform, video_id, title
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY platform ORDER BY total_views DESC) as rank
                FROM video_totals
                WHERE total_views > 0
            )
            SELECT platform, video_id, title, total_views, last_viewed
            FROM ranked
            WHERE rank <= 10
            ORDER BY platform, rank
        """
    },

    'engagement_brightcove': {
        'name': 'Engagement Metrics (Brightcove)',
        'description': 'Engagement funnel analysis for Brightcove',
        'sql': """
            SELECT
                channel,
                COUNT(DISTINCT video_id) as videos,
                ROUND(AVG(video_engagement_1), 1) as avg_start_pct,
                ROUND(AVG(video_engagement_25), 1) as avg_25_pct,
                ROUND(AVG(video_engagement_50), 1) as avg_50_pct,
                ROUND(AVG(video_engagement_75), 1) as avg_75_pct,
                ROUND(AVG(video_engagement_100), 1) as avg_completion_pct,
                ROUND(AVG(engagement_score), 2) as avg_engagement_score
            FROM unified_video_daily
            WHERE platform = 'brightcove'
                AND video_engagement_1 > 0
            GROUP BY channel
            ORDER BY avg_completion_pct DESC
        """
    },

    'browser_vbrick': {
        'name': 'Browser Usage (Vbrick)',
        'description': 'Browser breakdown for Vbrick platform',
        'sql': """
            SELECT
                DATE_TRUNC('month', date) as month,
                SUM(browser_chrome) as chrome_views,
                SUM(browser_edge) as edge_views,
                SUM(browser_other) as other_views,
                ROUND(SUM(browser_chrome) * 100.0 /
                    NULLIF(SUM(browser_chrome + browser_edge + browser_other), 0), 1) as chrome_pct
            FROM unified_video_daily
            WHERE platform = 'vbrick'
            GROUP BY DATE_TRUNC('month', date)
            ORDER BY month DESC
            LIMIT 12
        """
    },

    'webcasts_overview': {
        'name': 'Webcasts Overview',
        'description': 'Summary of Vbrick webcasts',
        'sql': """
            SELECT
                COUNT(*) as total_events,
                SUM(attendee_total) as total_attendance,
                ROUND(AVG(attendee_total), 0) as avg_attendance,
                SUM(zone_apac) as apac_attendance,
                SUM(zone_americas) as americas_attendance,
                SUM(zone_emea) as emea_attendance,
                SUM(zone_swiss) as swiss_attendance
            FROM unified_webcasts
        """
    },

    'webcast_vod': {
        'name': 'Webcast VOD Performance',
        'description': 'Compare live attendance vs recorded video views',
        'sql': """
            SELECT
                w.title,
                w.start_date,
                w.attendee_total as live_attendance,
                COALESCE(SUM(v.views), 0) as vod_views,
                CASE
                    WHEN w.attendee_total > 0
                    THEN ROUND(COALESCE(SUM(v.views), 0) * 1.0 / w.attendee_total, 2)
                    ELSE NULL
                END as vod_multiplier
            FROM unified_webcasts w
            LEFT JOIN unified_video_daily v
                ON w.vod_video_id = v.video_id
                AND v.platform = 'vbrick'
            WHERE w.vod_video_id IS NOT NULL
            GROUP BY w.event_id, w.title, w.start_date, w.attendee_total
            ORDER BY w.start_date DESC
            LIMIT 20
        """
    },

    'accounts': {
        'name': 'Account Overview',
        'description': 'List all accounts in the system',
        'sql': """
            SELECT
                platform,
                account_id,
                account_name,
                account_category
            FROM dim_accounts
            ORDER BY platform, account_name
        """
    },

    'date_coverage': {
        'name': 'Date Coverage by Platform',
        'description': 'Data availability by platform',
        'sql': """
            SELECT
                platform,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT date) as days_with_data,
                COUNT(DISTINCT video_id) as unique_videos
            FROM unified_video_daily
            GROUP BY platform
        """
    },
}


def run_query(conn, query_key: str, print_results: bool = True) -> list:
    """
    Run a named query and return results.

    Args:
        conn: DuckDB connection
        query_key: Key from QUERIES dict
        print_results: If True, print formatted results

    Returns:
        List of result rows
    """
    if query_key not in QUERIES:
        logger.error(f"Unknown query: {query_key}")
        return []

    query_info = QUERIES[query_key]
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Query: {query_info['name']}")
    logger.info(f"Description: {query_info['description']}")
    logger.info('=' * 60)

    try:
        result = conn.execute(query_info['sql']).fetchdf()

        if print_results:
            if result.empty:
                logger.info("(No results)")
            else:
                print(result.to_string(index=False))
                logger.info(f"\n({len(result)} rows)")

        return result.to_dict('records')

    except Exception as e:
        logger.error(f"Query failed: {e}")
        return []


def list_queries():
    """Print available queries."""
    logger.info("\nAvailable Queries:")
    logger.info("-" * 60)
    for key, info in QUERIES.items():
        logger.info(f"  {key:25} - {info['name']}")
    logger.info("-" * 60)
    logger.info(f"\nUsage: python query_examples.py --query <query_name>")


def main():
    parser = argparse.ArgumentParser(description='Run example cross-platform queries')
    parser.add_argument('--query', type=str, help='Run specific query by name')
    parser.add_argument('--list', action='store_true', help='List available queries')
    parser.add_argument('--all', action='store_true', help='Run all queries')
    args = parser.parse_args()

    if args.list:
        list_queries()
        return

    # Check database exists
    db_path = get_crossplatform_db_path()
    if not db_path.exists():
        logger.error(f"Database not found at {db_path}")
        logger.error("Run sync_all.py first to populate the database")
        return

    conn = init_crossplatform_db()

    try:
        if args.query:
            # Run specific query
            if args.query not in QUERIES:
                logger.error(f"Unknown query: {args.query}")
                list_queries()
                return
            run_query(conn, args.query)

        elif args.all:
            # Run all queries
            for query_key in QUERIES:
                run_query(conn, query_key)

        else:
            # Run a few key queries by default
            default_queries = ['views_by_platform', 'device_breakdown', 'date_coverage']
            logger.info("Running default queries (use --all for all queries)...")
            for query_key in default_queries:
                run_query(conn, query_key)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
