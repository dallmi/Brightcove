"""
debug_last_viewed.py - Check last viewed date for specific videos via API

Usage:
    python debug_last_viewed.py

This script queries the Brightcove Analytics API directly to find the
last view date for specified video IDs. Use this to compare against
Harper and Unified Pipeline outputs.
"""

import json
import time
from base64 import b64encode
import requests

# =============================================================================
# CONFIGURATION - Edit these values
# =============================================================================

# Video IDs to check (add your test video IDs here)
VIDEO_IDS = [
    "6aborvr5x8",  # Example - replace with real IDs
    "6365188656112",
    # Add more video IDs...
]

# Account to check (account_name: account_id)
# The script will check each video against each account
ACCOUNTS = {
    "impact": "968049871001",
    "intranet": "4413047246001",
    # Add more accounts as needed...
}

# =============================================================================
# AUTH (uses existing secrets.json)
# =============================================================================

def load_secrets():
    with open('secrets.json', 'r') as f:
        return json.load(f)


class BrightcoveAuth:
    def __init__(self, client_id, client_secret, proxies=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.proxies = proxies
        self.token = None
        self.token_time = 0

    def get_token(self):
        if self.token and (time.time() - self.token_time) < 300:
            return self.token

        auth = b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        resp = requests.post(
            "https://oauth.brightcove.com/v3/access_token",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            proxies=self.proxies
        )
        resp.raise_for_status()
        self.token = resp.json()["access_token"]
        self.token_time = time.time()
        return self.token


# =============================================================================
# API QUERIES
# =============================================================================

def get_last_view_date(auth, account_id, video_id, proxies=None):
    """
    Query Analytics API for the most recent view date of a specific video.

    Returns:
        tuple: (last_view_date, total_views) or (None, 0) if no views
    """
    url = "https://analytics.api.brightcove.com/v1/data"

    # Query for this specific video, sorted by date descending
    params = {
        "accounts": account_id,
        "dimensions": "video,date",
        "where": f"video=={video_id}",
        "fields": "video_view",
        "from": "alltime",
        "to": "now",
        "sort": "-date",
        "limit": 1,  # We only need the most recent
    }

    headers = {"Authorization": f"Bearer {auth.get_token()}"}

    try:
        resp = requests.get(url, headers=headers, params=params, proxies=proxies)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        if items and items[0].get("video_view", 0) > 0:
            return items[0]["date"], items[0]["video_view"]

        # No views found - let's also check total views for this video
        total_params = {
            "accounts": account_id,
            "dimensions": "video",
            "where": f"video=={video_id}",
            "fields": "video_view",
            "from": "alltime",
            "to": "now",
        }
        resp2 = requests.get(url, headers=headers, params=total_params, proxies=proxies)
        resp2.raise_for_status()
        data2 = resp2.json()
        items2 = data2.get("items", [])
        total_views = items2[0].get("video_view", 0) if items2 else 0

        return None, total_views

    except requests.exceptions.HTTPError as e:
        print(f"    API Error: {e}")
        return "ERROR", 0


def check_video_exists_in_cms(auth, account_id, video_id, proxies=None):
    """Check if video exists in CMS for this account."""
    url = f"https://cms.api.brightcove.com/v1/accounts/{account_id}/videos/{video_id}"
    headers = {"Authorization": f"Bearer {auth.get_token()}"}

    try:
        resp = requests.get(url, headers=headers, proxies=proxies)
        if resp.status_code == 200:
            data = resp.json()
            return True, data.get("name", "Unknown")
        elif resp.status_code == 404:
            return False, None
        else:
            return None, f"HTTP {resp.status_code}"
    except Exception as e:
        return None, str(e)


# =============================================================================
# MAIN
# =============================================================================

def main():
    secrets = load_secrets()
    proxies = secrets.get('proxies') if secrets.get('proxies') else None

    auth = BrightcoveAuth(
        secrets['client_id'],
        secrets['client_secret'],
        proxies
    )

    print("=" * 80)
    print("DEBUG: Last Viewed Date Check")
    print("=" * 80)
    print()

    results = []

    for account_name, account_id in ACCOUNTS.items():
        print(f"\n{'='*60}")
        print(f"Account: {account_name} ({account_id})")
        print("=" * 60)

        for video_id in VIDEO_IDS:
            print(f"\n  Video: {video_id}")

            # Check if video exists in this account's CMS
            exists, name = check_video_exists_in_cms(auth, account_id, video_id, proxies)

            if exists is False:
                print(f"    CMS: NOT FOUND in this account")
                continue
            elif exists is None:
                print(f"    CMS: Error checking - {name}")
                continue
            else:
                print(f"    CMS: Found - '{name}'")

            # Get last view date from Analytics API
            last_date, views = get_last_view_date(auth, account_id, video_id, proxies)

            if last_date == "ERROR":
                print(f"    Analytics: API Error")
            elif last_date is None:
                print(f"    Analytics: NO VIEWS (total_views={views})")
                print(f"    --> This would be NaN in Unified Pipeline")
            else:
                print(f"    Analytics: Last viewed on {last_date} (views on that day: {views})")

            results.append({
                "account": account_name,
                "video_id": video_id,
                "video_name": name,
                "dt_last_viewed": last_date,
                "has_views": last_date is not None and last_date != "ERROR"
            })

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\n{'Account':<20} {'Video ID':<20} {'Last Viewed':<15} {'Status'}")
    print("-" * 80)

    for r in results:
        status = "OK" if r["has_views"] else "NO VIEWS (NaN)"
        date_str = r["dt_last_viewed"] or "None"
        print(f"{r['account']:<20} {r['video_id']:<20} {date_str:<15} {status}")

    print("\n" + "=" * 80)
    print("Videos with 'NO VIEWS' will have dt_last_viewed = NaN in Unified Pipeline")
    print("=" * 80)


if __name__ == "__main__":
    main()
