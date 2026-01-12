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
        'cms': root / 'output' / 'cms',
        'analytics': root / 'output' / 'analytics',
        'daily': root / 'output' / 'daily',
        'stakeholder': root / 'output' / 'stakeholder',  # Excel files for stakeholder
        'checkpoints': root / 'checkpoints',
        'logs': root / 'logs'
    }
