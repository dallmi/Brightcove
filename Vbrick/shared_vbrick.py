"""
shared_vbrick.py - Shared utilities for the Vbrick Analytics Pipeline

Contains:
- VbrickAuthManager: Token management with auto-refresh
- safe_get: HTTP GET with retry logic
- DuckDB utilities: init_vbrick_db, upsert functions, get_db_stats
- Configuration loading
- Date utilities for incremental processing
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple

import requests
from requests.exceptions import ProxyError, ConnectionError


# =============================================================================
# PATH UTILITIES
# =============================================================================

def get_project_root() -> Path:
    """Get the Vbrick project root directory."""
    return Path(__file__).parent


def get_output_dir() -> Path:
    """Get the output directory for generated files."""
    output_dir = get_project_root() / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def get_vbrick_db_path() -> Path:
    """Get path to vbrick_analytics.duckdb."""
    return get_output_dir() / "vbrick_analytics.duckdb"


# =============================================================================
# CONFIGURATION
# =============================================================================

def load_vbrick_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load Vbrick configuration from secrets.json.

    Args:
        config_path: Optional path to config file. Uses VBRICK_CONFIG_JSON env var
                    or defaults to secrets.json in project root.

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = os.getenv("VBRICK_CONFIG_JSON", str(get_project_root() / "secrets.json"))

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        return json.load(f)


# =============================================================================
# HTTP UTILITIES
# =============================================================================

def safe_get(
    url: str,
    headers: Optional[Dict] = None,
    params: Optional[Dict] = None,
    proxies: Optional[str] = None,
    retries: int = 3,
    delay: float = 2.0,
    logger: Optional[logging.Logger] = None
) -> Optional[Dict]:
    """
    Safe HTTP GET with retry logic and error handling.

    Args:
        url: URL to fetch
        headers: HTTP headers
        params: Query parameters
        proxies: Proxy URL
        retries: Number of retry attempts
        delay: Initial delay between retries (exponential backoff)
        logger: Optional logger

    Returns:
        JSON response or None on failure
    """
    if logger is None:
        logger = logging.getLogger('HTTP')

    current_delay = delay

    for attempt in range(1, retries + 1):
        try:
            logger.debug(f"GET {url} (attempt {attempt}/{retries})")
            resp = requests.get(url, headers=headers, params=params, proxies=proxies, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except (ProxyError, ConnectionError) as e:
            logger.warning(f"Attempt {attempt}/{retries} network error: {e}")
        except requests.HTTPError as e:
            logger.error(f"HTTP {e.response.status_code} on GET {url}: {e.response.text}")
            if e.response.status_code == 429:
                current_delay *= 2  # Exponential backoff for rate limits
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{retries} failed: {e}")

        if attempt < retries:
            time.sleep(current_delay)
            current_delay *= 1.5  # Exponential backoff

    logger.error(f"All {retries} attempts failed for GET {url}")
    return None


# =============================================================================
# AUTHENTICATION
# =============================================================================

class VbrickAuthManager:
    """
    Manages Vbrick API authentication with automatic token refresh.

    Tokens are refreshed when they have less than 60 seconds until expiry.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        proxies: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.api_secret = api_secret
        self.proxies = proxies
        self.logger = logger or logging.getLogger('VbrickAuth')

        self.token = None
        self.token_created = 0
        self.expires_in = 3600

    def get_token(self) -> str:
        """Get a valid token, refreshing if needed."""
        now = time.time()
        if not self.token or (now - self.token_created) > (self.expires_in - 60):
            self._refresh_token()
        return self.token

    def _refresh_token(self):
        """Request a new token from the Vbrick API."""
        url = f"{self.base_url}/api/v2/authenticate"
        self.logger.info(f"Requesting new access token via {url}")

        headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        payload = {
            "apiKey": self.api_key,
            "apiSecret": self.api_secret
        }

        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                proxies=self.proxies,
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as e:
            self.logger.error(f"Authentication failed {e.response.status_code}: {e.response.text}")
            raise
        except Exception as ex:
            self.logger.error(f"Unexpected error fetching token: {ex}")
            raise

        token = data.get("token")
        if not token:
            self.logger.error(f"No 'token' field in response: {data}")
            raise ValueError("No token in authentication response")

        self.token = token
        self.expires_in = data.get("expiresIn", self.expires_in)
        self.token_created = time.time()
        self.logger.info(f"Obtained token; expires in {self.expires_in} seconds")


# =============================================================================
# DUCKDB UTILITIES
# =============================================================================

def init_vbrick_db(db_path: Optional[Path] = None) -> 'duckdb.DuckDBPyConnection':
    """
    Initialize the Vbrick DuckDB database with required tables.

    Creates tables if they don't exist:
    - vbrick_video_daily: Daily video analytics
    - vbrick_webcasts: Webcast event data

    Args:
        db_path: Optional path to database file

    Returns:
        DuckDB connection
    """
    import duckdb

    if db_path is None:
        db_path = get_vbrick_db_path()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # Create vbrick_video_daily table (daily video analytics)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vbrick_video_daily (
            -- Primary key columns
            video_id VARCHAR NOT NULL,
            date DATE NOT NULL,

            -- Video metadata
            title VARCHAR,
            playback_url VARCHAR,
            duration INTEGER,
            when_uploaded VARCHAR,
            last_viewed VARCHAR,
            when_published VARCHAR,
            uploaded_by VARCHAR,
            tags VARCHAR,
            comment_count INTEGER,
            score DOUBLE,

            -- View metrics
            views INTEGER DEFAULT 0,

            -- Device breakdown
            device_desktop INTEGER DEFAULT 0,
            device_mobile INTEGER DEFAULT 0,
            device_other INTEGER DEFAULT 0,

            -- Browser breakdown
            browser_chrome INTEGER DEFAULT 0,
            browser_edge INTEGER DEFAULT 0,
            browser_other INTEGER DEFAULT 0,

            -- Meta columns
            report_generated_on VARCHAR,

            PRIMARY KEY (video_id, date)
        )
    """)

    # Create vbrick_webcasts table (webcast event data)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vbrick_webcasts (
            -- Primary key
            event_id VARCHAR NOT NULL PRIMARY KEY,

            -- Event metadata
            title VARCHAR,
            vod_id VARCHAR,
            event_url VARCHAR,
            start_date VARCHAR,
            end_date VARCHAR,

            -- Attendance metrics
            attendee_count INTEGER DEFAULT 0,
            attendee_total INTEGER DEFAULT 0,
            total_viewing_time INTEGER DEFAULT 0,

            -- Zone breakdown
            zone_apac INTEGER DEFAULT 0,
            zone_america INTEGER DEFAULT 0,
            zone_emea INTEGER DEFAULT 0,
            zone_swiss INTEGER DEFAULT 0,
            zone_other INTEGER DEFAULT 0,

            -- Browser breakdown
            browser_chrome INTEGER DEFAULT 0,
            browser_edge INTEGER DEFAULT 0,
            browser_other INTEGER DEFAULT 0,

            -- Device breakdown
            device_pc INTEGER DEFAULT 0,
            device_mobile INTEGER DEFAULT 0,
            device_other INTEGER DEFAULT 0,

            -- AI categorization
            category VARCHAR,
            subcategory VARCHAR,

            -- Meta columns
            report_generated_on VARCHAR
        )
    """)

    # Create indexes for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_vbrick_video_date
        ON vbrick_video_daily (date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_vbrick_video_uploaded
        ON vbrick_video_daily (when_uploaded)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_vbrick_webcast_vod
        ON vbrick_webcasts (vod_id)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_vbrick_webcast_start
        ON vbrick_webcasts (start_date)
    """)

    return conn


def upsert_video_daily(
    conn: 'duckdb.DuckDBPyConnection',
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Upsert rows into vbrick_video_daily table.

    Uses INSERT OR REPLACE to handle duplicates (same video_id, date).

    Args:
        conn: DuckDB connection
        rows: List of row dictionaries
        logger: Optional logger

    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0

    if logger is None:
        logger = logging.getLogger('DuckDB')

    # Define column order matching table schema
    columns = [
        'video_id', 'date', 'title', 'playback_url', 'duration',
        'when_uploaded', 'last_viewed', 'when_published', 'uploaded_by', 'tags',
        'comment_count', 'score', 'views',
        'device_desktop', 'device_mobile', 'device_other',
        'browser_chrome', 'browser_edge', 'browser_other',
        'report_generated_on'
    ]

    # Build INSERT OR REPLACE statement
    placeholders = ', '.join(['?' for _ in columns])
    column_names = ', '.join(columns)

    sql = f"""
        INSERT OR REPLACE INTO vbrick_video_daily ({column_names})
        VALUES ({placeholders})
    """

    # Convert rows to tuples
    values = []
    for row in rows:
        row_tuple = tuple(row.get(col, None) for col in columns)
        values.append(row_tuple)

    # Execute batch insert
    conn.executemany(sql, values)

    logger.debug(f"Upserted {len(rows)} rows into vbrick_video_daily")
    return len(rows)


def upsert_webcasts(
    conn: 'duckdb.DuckDBPyConnection',
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Upsert rows into vbrick_webcasts table.

    Uses INSERT OR REPLACE to handle duplicates (same event_id).

    Args:
        conn: DuckDB connection
        rows: List of row dictionaries
        logger: Optional logger

    Returns:
        Number of rows upserted
    """
    if not rows:
        return 0

    if logger is None:
        logger = logging.getLogger('DuckDB')

    # Define column order matching table schema
    columns = [
        'event_id', 'title', 'vod_id', 'event_url', 'start_date', 'end_date',
        'attendee_count', 'attendee_total', 'total_viewing_time',
        'zone_apac', 'zone_america', 'zone_emea', 'zone_swiss', 'zone_other',
        'browser_chrome', 'browser_edge', 'browser_other',
        'device_pc', 'device_mobile', 'device_other',
        'category', 'subcategory', 'report_generated_on'
    ]

    # Build INSERT OR REPLACE statement
    placeholders = ', '.join(['?' for _ in columns])
    column_names = ', '.join(columns)

    sql = f"""
        INSERT OR REPLACE INTO vbrick_webcasts ({column_names})
        VALUES ({placeholders})
    """

    # Convert rows to tuples
    values = []
    for row in rows:
        row_tuple = tuple(row.get(col, None) for col in columns)
        values.append(row_tuple)

    # Execute batch insert
    conn.executemany(sql, values)

    logger.debug(f"Upserted {len(rows)} rows into vbrick_webcasts")
    return len(rows)


def get_video_max_date(
    conn: 'duckdb.DuckDBPyConnection',
    video_id: str
) -> Optional[str]:
    """
    Get the maximum date for a video in the database.

    Used for incremental updates with overlap.

    Args:
        conn: DuckDB connection
        video_id: Video ID

    Returns:
        Date string (YYYY-MM-DD) or None if no data exists
    """
    result = conn.execute("""
        SELECT MAX(date)::VARCHAR
        FROM vbrick_video_daily
        WHERE video_id = ?
    """, [video_id]).fetchone()

    return result[0] if result and result[0] else None


def get_all_video_max_dates(
    conn: 'duckdb.DuckDBPyConnection'
) -> Dict[str, str]:
    """
    Get max dates for all videos in the database.

    Args:
        conn: DuckDB connection

    Returns:
        Dict mapping video_id -> max_date
    """
    result = conn.execute("""
        SELECT video_id, MAX(date)::VARCHAR as max_date
        FROM vbrick_video_daily
        GROUP BY video_id
    """).fetchall()

    return {row[0]: row[1] for row in result}


def get_existing_webcast_ids(
    conn: 'duckdb.DuckDBPyConnection'
) -> set:
    """
    Get all existing webcast event IDs in the database.

    Used for incremental updates (skip already-processed events).

    Args:
        conn: DuckDB connection

    Returns:
        Set of event IDs
    """
    result = conn.execute("""
        SELECT event_id FROM vbrick_webcasts
    """).fetchall()

    return {row[0] for row in result}


def get_db_stats(conn: 'duckdb.DuckDBPyConnection') -> Dict[str, Any]:
    """
    Get statistics about both Vbrick tables.

    Returns dict with:
    - video_daily: Stats for vbrick_video_daily table
    - webcasts: Stats for vbrick_webcasts table
    """
    stats = {'video_daily': {}, 'webcasts': {}}

    # Video daily stats
    result = conn.execute("SELECT COUNT(*) FROM vbrick_video_daily").fetchone()
    stats['video_daily']['total_rows'] = result[0] if result else 0

    result = conn.execute("SELECT COUNT(DISTINCT video_id) FROM vbrick_video_daily").fetchone()
    stats['video_daily']['unique_videos'] = result[0] if result else 0

    result = conn.execute("""
        SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM vbrick_video_daily
    """).fetchone()
    stats['video_daily']['date_range'] = (result[0], result[1]) if result else (None, None)

    # Webcast stats
    result = conn.execute("SELECT COUNT(*) FROM vbrick_webcasts").fetchone()
    stats['webcasts']['total_events'] = result[0] if result else 0

    result = conn.execute("SELECT COUNT(*) FROM vbrick_webcasts WHERE vod_id IS NOT NULL AND vod_id != ''").fetchone()
    stats['webcasts']['events_with_video'] = result[0] if result else 0

    result = conn.execute("""
        SELECT MIN(start_date), MAX(start_date) FROM vbrick_webcasts
    """).fetchone()
    stats['webcasts']['date_range'] = (result[0], result[1]) if result else (None, None)

    return stats


def print_db_stats(db_path: Optional[Path] = None, logger: Optional[logging.Logger] = None):
    """
    Print database statistics in a formatted way.

    Args:
        db_path: Optional path to database
        logger: Optional logger
    """
    import duckdb

    if logger is None:
        logger = logging.getLogger('DuckDB')

    if db_path is None:
        db_path = get_vbrick_db_path()

    if not db_path.exists():
        logger.info(f"Database not found: {db_path}")
        return

    # Get file size
    size_mb = db_path.stat().st_size / (1024 * 1024)

    conn = duckdb.connect(str(db_path), read_only=True)
    stats = get_db_stats(conn)
    conn.close()

    logger.info("=" * 60)
    logger.info(f"Database: {db_path}")
    logger.info(f"Size: {size_mb:.2f} MB")
    logger.info("=" * 60)

    logger.info("\nVideo Daily Analytics (vbrick_video_daily):")
    logger.info(f"  Total rows: {stats['video_daily']['total_rows']:,}")
    logger.info(f"  Unique videos: {stats['video_daily']['unique_videos']:,}")
    logger.info(f"  Date range: {stats['video_daily']['date_range'][0]} to {stats['video_daily']['date_range'][1]}")

    logger.info("\nWebcasts (vbrick_webcasts):")
    logger.info(f"  Total events: {stats['webcasts']['total_events']:,}")
    logger.info(f"  Events with video: {stats['webcasts']['events_with_video']:,}")
    logger.info(f"  Date range: {stats['webcasts']['date_range'][0]} to {stats['webcasts']['date_range'][1]}")

    logger.info("=" * 60)


def calculate_overlap_start_date(
    last_processed_date: Optional[str],
    default_start: str,
    overlap_days: int = 7
) -> str:
    """
    Calculate the start date for fetching with overlap.

    For API lag compensation, starts N days before the last processed date.

    Args:
        last_processed_date: Last date in database (YYYY-MM-DD) or None
        default_start: Default start date if no data exists (YYYY-MM-DD)
        overlap_days: Number of days to overlap (default 7)

    Returns:
        Start date string (YYYY-MM-DD)
    """
    if not last_processed_date:
        return default_start

    last_dt = datetime.strptime(last_processed_date, "%Y-%m-%d")
    overlap_dt = last_dt - timedelta(days=overlap_days)
    default_start_dt = datetime.strptime(default_start, "%Y-%m-%d")

    # Don't go before default start
    start_dt = max(overlap_dt, default_start_dt)

    return start_dt.strftime("%Y-%m-%d")


# =============================================================================
# EXPORT UTILITIES
# =============================================================================

def export_to_parquet(
    conn: 'duckdb.DuckDBPyConnection',
    output_dir: Optional[Path] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Path]:
    """
    Export Vbrick tables to Parquet files.

    Args:
        conn: DuckDB connection
        output_dir: Output directory (defaults to output/parquet)
        logger: Optional logger

    Returns:
        Dict mapping table name to output path
    """
    if logger is None:
        logger = logging.getLogger('DuckDB')

    if output_dir is None:
        output_dir = get_output_dir() / 'parquet'

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    paths = {}

    # Export video daily
    video_path = output_dir / 'vbrick_video_daily.parquet'
    conn.execute(f"""
        COPY (SELECT * FROM vbrick_video_daily ORDER BY video_id, date)
        TO '{video_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    paths['vbrick_video_daily'] = video_path
    logger.info(f"Exported vbrick_video_daily to {video_path}")

    # Export webcasts
    webcast_path = output_dir / 'vbrick_webcasts.parquet'
    conn.execute(f"""
        COPY (SELECT * FROM vbrick_webcasts ORDER BY start_date, event_id)
        TO '{webcast_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    paths['vbrick_webcasts'] = webcast_path
    logger.info(f"Exported vbrick_webcasts to {webcast_path}")

    return paths
