"""
shared.py - Shared utilities for the UnifiedPipeline

Contains:
- BrightcoveAuthManager: Token management with auto-refresh
- robust_api_call: API calls with retry, backoff, and jitter
- Checkpoint utilities: Atomic save/load for resume capability
- Configuration loading
"""

import json
import os
import time
import random
import logging
from base64 import b64encode
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import requests
from requests.exceptions import RequestException, HTTPError

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(log_dir: Path, script_name: str) -> logging.Logger:
    """Setup logging with both file and console output."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{script_name}_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)

    # File handler
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)-5s [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# =============================================================================
# CONFIGURATION
# =============================================================================

def get_project_root() -> Path:
    """Get the UnifiedPipeline root directory."""
    return Path(__file__).parent.parent


def load_config(test_mode: bool = None) -> Dict[str, Any]:
    """
    Load all configuration files.

    Args:
        test_mode: If True, use *_TEST.json configs. If None, check PIPELINE_TEST env var.

    Environment:
        PIPELINE_TEST=1  -> Use test configs (accounts_TEST.json, settings_TEST.json)
    """
    root = get_project_root()
    config = {}

    # Determine mode
    if test_mode is None:
        test_mode = os.environ.get('PIPELINE_TEST', '').lower() in ('1', 'true', 'yes')

    suffix = "_TEST" if test_mode else ""

    if test_mode:
        print(f"*** TEST MODE ACTIVE - Using *_TEST.json configs ***")

    # Load accounts
    accounts_path = root / "config" / f"accounts{suffix}.json"
    if not accounts_path.exists() and test_mode:
        raise FileNotFoundError(f"Test config not found: {accounts_path}")
    with open(accounts_path, 'r', encoding='utf-8') as f:
        config['accounts'] = json.load(f)

    # Load settings
    settings_path = root / "config" / f"settings{suffix}.json"
    if not settings_path.exists() and test_mode:
        raise FileNotFoundError(f"Test config not found: {settings_path}")
    with open(settings_path, 'r', encoding='utf-8') as f:
        config['settings'] = json.load(f)

    return config


def load_secrets() -> Dict[str, Any]:
    """Load secrets from the parent Brightcove directory."""
    # Look for secrets.json in the parent directory (Brightcove folder)
    secrets_path = get_project_root().parent / "secrets.json"
    if not secrets_path.exists():
        # Also check current working directory
        secrets_path = Path("secrets.json")

    if not secrets_path.exists():
        raise FileNotFoundError(
            f"secrets.json not found. Expected at {secrets_path} or ./secrets.json"
        )

    with open(secrets_path, 'r', encoding='utf-8') as f:
        return json.load(f)


# =============================================================================
# AUTHENTICATION
# =============================================================================

class BrightcoveAuthManager:
    """
    Manages Brightcove OAuth tokens with automatic refresh.

    Features:
    - Automatic token refresh before expiry
    - Thread-safe token access
    - Configurable refresh buffer
    """

    def __init__(self, client_id: str, client_secret: str,
                 proxies: Optional[Dict] = None,
                 refresh_buffer_seconds: int = 30):
        self.client_id = client_id
        self.client_secret = client_secret
        self.proxies = proxies
        self.refresh_buffer = refresh_buffer_seconds

        self.token: Optional[str] = None
        self.token_created_at: float = 0
        self.token_expires_in: int = 300  # Default 5 minutes

        self.logger = logging.getLogger('AuthManager')

    def get_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        if self._is_token_valid():
            return self.token
        return self._refresh_token()

    def _is_token_valid(self) -> bool:
        """Check if current token is still valid."""
        if not self.token:
            return False
        elapsed = time.time() - self.token_created_at
        return elapsed < (self.token_expires_in - self.refresh_buffer)

    def _refresh_token(self) -> str:
        """Refresh the access token."""
        self.logger.info("Refreshing access token...")

        auth = b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}

        response = requests.post(
            "https://oauth.brightcove.com/v3/access_token",
            headers=headers,
            data=data,
            proxies=self.proxies
        )
        response.raise_for_status()

        result = response.json()
        self.token = result.get('access_token')
        self.token_expires_in = result.get('expires_in', 300)
        self.token_created_at = time.time()

        self.logger.info("Access token refreshed successfully")
        return self.token


# =============================================================================
# ROBUST API CALLS
# =============================================================================

class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(self,
                 max_retries: int = 5,
                 initial_delay: float = 2.0,
                 max_delay: float = 120.0,
                 exponential_base: float = 2.0,
                 jitter_factor: float = 0.5,
                 retryable_codes: Optional[List[int]] = None):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter_factor = jitter_factor
        self.retryable_codes = retryable_codes or [429, 500, 502, 503, 504]

    @classmethod
    def from_settings(cls, settings: Dict) -> 'RetryConfig':
        """Create RetryConfig from settings dict."""
        retry_settings = settings.get('retry', {})
        return cls(
            max_retries=retry_settings.get('max_retries', 5),
            initial_delay=retry_settings.get('initial_delay_seconds', 2.0),
            max_delay=retry_settings.get('max_delay_seconds', 120.0),
            exponential_base=retry_settings.get('exponential_base', 2.0),
            jitter_factor=retry_settings.get('jitter_factor', 0.5),
            retryable_codes=retry_settings.get('retryable_status_codes', [429, 500, 502, 503, 504])
        )


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate delay with exponential backoff and jitter."""
    base_delay = config.initial_delay * (config.exponential_base ** attempt)
    base_delay = min(base_delay, config.max_delay)

    # Add jitter
    jitter = base_delay * config.jitter_factor * random.random()
    return base_delay + jitter


def robust_api_call(
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict] = None,
    retry_config: Optional[RetryConfig] = None,
    proxies: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None
) -> Optional[requests.Response]:
    """
    Make an API call with robust retry logic.

    Features:
    - Exponential backoff with jitter
    - Handles rate limits (429) with Retry-After header
    - Configurable retry behavior
    - Detailed logging

    Returns:
        Response object on success, None on permanent failure
    """
    if retry_config is None:
        retry_config = RetryConfig()

    if logger is None:
        logger = logging.getLogger('API')

    last_exception = None

    for attempt in range(retry_config.max_retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                proxies=proxies,
                timeout=60
            )

            # Success
            if response.status_code == 200:
                return response

            # Rate limit - respect Retry-After header
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    wait_time = int(retry_after) + random.uniform(0, 5)
                else:
                    wait_time = calculate_delay(attempt, retry_config)

                logger.warning(
                    f"Rate limited (429). Waiting {wait_time:.1f}s "
                    f"(attempt {attempt + 1}/{retry_config.max_retries})"
                )
                time.sleep(wait_time)
                continue

            # Other retryable errors
            if response.status_code in retry_config.retryable_codes:
                delay = calculate_delay(attempt, retry_config)
                logger.warning(
                    f"Retryable error {response.status_code}. "
                    f"Waiting {delay:.1f}s (attempt {attempt + 1}/{retry_config.max_retries})"
                )
                time.sleep(delay)
                continue

            # Non-retryable error
            logger.error(
                f"Non-retryable error {response.status_code}: {response.text[:200]}"
            )
            response.raise_for_status()

        except RequestException as e:
            last_exception = e
            delay = calculate_delay(attempt, retry_config)
            logger.warning(
                f"Request failed: {e}. "
                f"Waiting {delay:.1f}s (attempt {attempt + 1}/{retry_config.max_retries})"
            )
            time.sleep(delay)

    # All retries exhausted
    logger.error(
        f"All {retry_config.max_retries} retries exhausted. Last error: {last_exception}"
    )
    return None


# =============================================================================
# CHECKPOINT UTILITIES
# =============================================================================

def save_checkpoint_atomic(path: Path, data: Dict[str, Any]) -> None:
    """
    Save checkpoint atomically to prevent corruption on crash.

    Uses write-to-temp-then-rename pattern for atomic writes.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix('.tmp')

    with open(temp_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())

    # Atomic rename (on POSIX systems)
    os.replace(temp_path, path)


def load_checkpoint(path: Path) -> Optional[Dict[str, Any]]:
    """Load checkpoint file if it exists."""
    path = Path(path)
    if not path.exists():
        return None

    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def append_checkpoint_line(path: Path, data: Dict[str, Any]) -> None:
    """Append a line to a JSONL checkpoint file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(data, default=str) + '\n')
        f.flush()


def load_checkpoint_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Load all lines from a JSONL checkpoint file."""
    path = Path(path)
    if not path.exists():
        return []

    rows = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# =============================================================================
# DATE UTILITIES
# =============================================================================

def generate_windows(
    start_date: str,
    end_date: str,
    window_type: str = 'quarterly'
) -> List[Tuple[str, str]]:
    """
    Generate time windows for analytics fetching.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD) or 'now'
        window_type: 'monthly', 'quarterly', or 'half_yearly'

    Returns:
        List of (from_date, to_date) tuples
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")

    if end_date == 'now':
        end_dt = datetime.now()
    else:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    windows = []
    current = start_dt

    while current < end_dt:
        if window_type == 'monthly':
            # End of current month
            if current.month == 12:
                next_start = datetime(current.year + 1, 1, 1)
            else:
                next_start = datetime(current.year, current.month + 1, 1)
            window_end = next_start - timedelta(days=1)

        elif window_type == 'quarterly':
            # End of current quarter
            quarter_end_month = ((current.month - 1) // 3 + 1) * 3
            if quarter_end_month > 12:
                quarter_end_month = 12

            if quarter_end_month == 12:
                next_start = datetime(current.year + 1, 1, 1)
            else:
                next_start = datetime(current.year, quarter_end_month + 1, 1)
            window_end = next_start - timedelta(days=1)

        else:  # half_yearly
            # End of current half-year
            if current.month <= 6:
                window_end = datetime(current.year, 6, 30)
                next_start = datetime(current.year, 7, 1)
            else:
                window_end = datetime(current.year, 12, 31)
                next_start = datetime(current.year + 1, 1, 1)

        # Don't go past end_dt
        if window_end > end_dt:
            window_end = end_dt

        # Determine if this is a "live" window
        is_live = window_end.date() >= datetime.now().date()
        to_str = 'now' if is_live else window_end.strftime("%Y-%m-%d")

        windows.append((current.strftime("%Y-%m-%d"), to_str))

        if window_end >= end_dt:
            break

        current = next_start

    return windows


def split_window(from_date: str, to_date: str) -> List[Tuple[str, str]]:
    """Split a window in half."""
    start = datetime.strptime(from_date, "%Y-%m-%d")

    if to_date == 'now':
        end = datetime.now()
    else:
        end = datetime.strptime(to_date, "%Y-%m-%d")

    days = (end - start).days
    if days <= 1:
        # Cannot split further
        return [(from_date, to_date)]

    mid = start + timedelta(days=days // 2)
    next_day = mid + timedelta(days=1)

    # Preserve 'now' for the second half if applicable
    to_str = to_date if to_date == 'now' else to_date

    return [
        (from_date, mid.strftime("%Y-%m-%d")),
        (next_day.strftime("%Y-%m-%d"), to_str)
    ]


def get_date_range_days(from_date: str, to_date: str) -> int:
    """Calculate the number of days in a date range."""
    start = datetime.strptime(from_date, "%Y-%m-%d")
    if to_date == 'now':
        end = datetime.now()
    else:
        end = datetime.strptime(to_date, "%Y-%m-%d")
    return (end - start).days


# =============================================================================
# OUTPUT PATH UTILITIES
# =============================================================================

def get_output_paths() -> Dict[str, Path]:
    """Get standard output directory paths."""
    root = get_project_root()
    return {
        'root': root,  # Project root directory
        'output': root / 'output',  # Base output directory
        'cms': root / 'output' / 'cms',
        'analytics': root / 'output' / 'analytics',
        'daily': root / 'output' / 'daily',
        'parquet': root / 'output' / 'parquet',  # Parquet output for PowerBI
        'life_cycle_mgmt': root / 'output' / 'life_cycle_mgmt',  # Excel files for lifecycle management
        'checkpoints': root / 'checkpoints',
        'logs': root / 'logs'
    }


# =============================================================================
# DUCKDB CHECKPOINT UTILITIES
# =============================================================================

def get_analytics_db_path() -> Path:
    """Get path to the central analytics DuckDB database."""
    return get_output_paths()['output'] / "analytics.duckdb"


def init_analytics_db(db_path: Optional[Path] = None) -> 'duckdb.DuckDBPyConnection':
    """
    Initialize the analytics DuckDB database with required tables.

    Creates tables if they don't exist:
    - daily_analytics: Daily video metrics (facts)
    - video_metadata: Video metadata (dimensions)

    Returns:
        DuckDB connection
    """
    import duckdb

    if db_path is None:
        db_path = get_analytics_db_path()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    # Create daily_analytics table (facts) with composite primary key
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_analytics (
            -- Primary key columns
            account_id VARCHAR NOT NULL,
            video_id VARCHAR NOT NULL,
            date DATE NOT NULL,

            -- Identifiers
            channel VARCHAR,
            name VARCHAR,

            -- View metrics
            video_view INTEGER DEFAULT 0,
            views_desktop INTEGER DEFAULT 0,
            views_mobile INTEGER DEFAULT 0,
            views_tablet INTEGER DEFAULT 0,
            views_other INTEGER DEFAULT 0,

            -- Engagement metrics
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

            -- CMS metadata
            created_at VARCHAR,
            published_at VARCHAR,
            original_filename VARCHAR,
            created_by VARCHAR,
            video_duration INTEGER,
            video_content_type VARCHAR,
            video_length VARCHAR,
            video_category VARCHAR,
            country VARCHAR,
            language VARCHAR,
            business_unit VARCHAR,
            tags VARCHAR,
            reference_id VARCHAR,
            dt_last_viewed VARCHAR,

            -- Harper additional fields
            cf_relatedlinkname VARCHAR,
            cf_relatedlink VARCHAR,
            cf_video_owner_email VARCHAR,
            cf_1a_comms_sign_off VARCHAR,
            cf_1b_comms_sign_off_approver VARCHAR,
            cf_2a_data_classification_disclaimer VARCHAR,
            cf_3a_records_management_disclaimer VARCHAR,
            cf_4a_archiving_disclaimer_comms_branding VARCHAR,
            cf_4b_unique_sharepoint_id VARCHAR,

            -- Meta columns
            report_generated_on VARCHAR,
            data_type VARCHAR,

            -- Primary key constraint
            PRIMARY KEY (account_id, video_id, date)
        )
    """)

    # Create index for common queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_analytics_video
        ON daily_analytics (video_id, date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_analytics_account_date
        ON daily_analytics (account_id, date)
    """)

    return conn


def upsert_daily_analytics(
    conn: 'duckdb.DuckDBPyConnection',
    rows: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None
) -> int:
    """
    Upsert rows into daily_analytics table.

    Uses INSERT OR REPLACE to handle duplicates (same account_id, video_id, date).

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
        'account_id', 'video_id', 'date', 'channel', 'name',
        'video_view', 'views_desktop', 'views_mobile', 'views_tablet', 'views_other',
        'video_impression', 'play_rate', 'engagement_score',
        'video_engagement_1', 'video_engagement_25', 'video_engagement_50',
        'video_engagement_75', 'video_engagement_100',
        'video_percent_viewed', 'video_seconds_viewed',
        'created_at', 'published_at', 'original_filename', 'created_by',
        'video_duration', 'video_content_type', 'video_length', 'video_category',
        'country', 'language', 'business_unit', 'tags', 'reference_id', 'dt_last_viewed',
        'cf_relatedlinkname', 'cf_relatedlink', 'cf_video_owner_email',
        'cf_1a_comms_sign_off', 'cf_1b_comms_sign_off_approver',
        'cf_2a_data_classification_disclaimer', 'cf_3a_records_management_disclaimer',
        'cf_4a_archiving_disclaimer_comms_branding', 'cf_4b_unique_sharepoint_id',
        'report_generated_on', 'data_type'
    ]

    # Build INSERT OR REPLACE statement
    placeholders = ', '.join(['?' for _ in columns])
    column_names = ', '.join(columns)

    sql = f"""
        INSERT OR REPLACE INTO daily_analytics ({column_names})
        VALUES ({placeholders})
    """

    # Convert rows to tuples
    values = []
    for row in rows:
        row_tuple = tuple(row.get(col, None) for col in columns)
        values.append(row_tuple)

    # Execute batch insert
    conn.executemany(sql, values)

    return len(rows)


def get_max_date_for_video(
    conn: 'duckdb.DuckDBPyConnection',
    account_id: str,
    video_id: str
) -> Optional[str]:
    """
    Get the maximum date for a video in the database.

    Used for incremental updates with overlap.

    Returns:
        Date string (YYYY-MM-DD) or None if no data exists
    """
    result = conn.execute("""
        SELECT MAX(date)::VARCHAR
        FROM daily_analytics
        WHERE account_id = ? AND video_id = ?
    """, [account_id, video_id]).fetchone()

    return result[0] if result and result[0] else None


def get_all_video_max_dates(
    conn: 'duckdb.DuckDBPyConnection',
    account_id: Optional[str] = None
) -> Dict[Tuple[str, str], str]:
    """
    Get max dates for all videos in the database.

    Args:
        conn: DuckDB connection
        account_id: Optional filter by account

    Returns:
        Dict mapping (account_id, video_id) -> max_date
    """
    if account_id:
        result = conn.execute("""
            SELECT account_id, video_id, MAX(date)::VARCHAR as max_date
            FROM daily_analytics
            WHERE account_id = ?
            GROUP BY account_id, video_id
        """, [account_id]).fetchall()
    else:
        result = conn.execute("""
            SELECT account_id, video_id, MAX(date)::VARCHAR as max_date
            FROM daily_analytics
            GROUP BY account_id, video_id
        """).fetchall()

    return {(row[0], row[1]): row[2] for row in result}


def get_db_stats(conn: 'duckdb.DuckDBPyConnection') -> Dict[str, Any]:
    """
    Get statistics about the database.

    Returns dict with:
    - total_rows: Total rows in daily_analytics
    - unique_videos: Number of unique videos
    - date_range: (min_date, max_date)
    - rows_by_account: Dict of account -> row_count
    """
    stats = {}

    # Total rows
    result = conn.execute("SELECT COUNT(*) FROM daily_analytics").fetchone()
    stats['total_rows'] = result[0] if result else 0

    # Unique videos
    result = conn.execute("SELECT COUNT(DISTINCT video_id) FROM daily_analytics").fetchone()
    stats['unique_videos'] = result[0] if result else 0

    # Date range
    result = conn.execute("""
        SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR FROM daily_analytics
    """).fetchone()
    stats['date_range'] = (result[0], result[1]) if result else (None, None)

    # Rows by account
    result = conn.execute("""
        SELECT account_id, COUNT(*) as cnt
        FROM daily_analytics
        GROUP BY account_id
    """).fetchall()
    stats['rows_by_account'] = {row[0]: row[1] for row in result}

    return stats


def export_to_parquet(
    conn: 'duckdb.DuckDBPyConnection',
    output_dir: Path,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Path]:
    """
    Export daily_analytics to Parquet files.

    Creates:
    - facts/daily_analytics_all.parquet
    - dimensions/video_metadata.parquet (aggregated from CMS)

    Returns:
        Dict mapping table name to output path
    """
    if logger is None:
        logger = logging.getLogger('DuckDB')

    output_dir = Path(output_dir)
    facts_dir = output_dir / 'facts'
    facts_dir.mkdir(parents=True, exist_ok=True)

    facts_path = facts_dir / 'daily_analytics_all.parquet'

    # Export facts
    conn.execute(f"""
        COPY (SELECT * FROM daily_analytics ORDER BY account_id, video_id, date)
        TO '{facts_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    logger.info(f"Exported daily_analytics to {facts_path}")

    return {'daily_analytics': facts_path}


def calculate_overlap_start_date(
    last_processed_date: Optional[str],
    year_start: str,
    overlap_days: int = 7
) -> str:
    """
    Calculate the start date for fetching with overlap.

    For lag compensation, starts N days before the last processed date.

    Args:
        last_processed_date: Last date in checkpoint (YYYY-MM-DD) or None
        year_start: Start of year (YYYY-MM-DD)
        overlap_days: Number of days to overlap (default 7)

    Returns:
        Start date string (YYYY-MM-DD)
    """
    if not last_processed_date:
        return year_start

    last_dt = datetime.strptime(last_processed_date, "%Y-%m-%d")
    overlap_dt = last_dt - timedelta(days=overlap_days)
    year_start_dt = datetime.strptime(year_start, "%Y-%m-%d")

    # Don't go before year start
    start_dt = max(overlap_dt, year_start_dt)

    return start_dt.strftime("%Y-%m-%d")
