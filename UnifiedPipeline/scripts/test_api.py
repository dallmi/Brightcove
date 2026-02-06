"""
Quick API test to verify Brightcove Analytics API is working.
Run: python test_api.py
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from shared import (
    load_config,
    load_secrets,
    BrightcoveAuthManager,
    RetryConfig,
    robust_api_call,
)

def main():
    script_dir = Path(__file__).parent
    config = load_config(script_dir.parent / 'config')
    secrets = load_secrets(script_dir.parent)

    # Get Internet account
    account_id = config['accounts']['Internet']['account_id']
    print(f"Testing with account: Internet ({account_id})")

    # Auth
    auth_manager = BrightcoveAuthManager(
        client_id=secrets['client_id'],
        client_secret=secrets['client_secret'],
        proxies=secrets.get('proxies', {})
    )

    token = auth_manager.get_token()
    print(f"Token obtained: {token[:20]}...")

    # Pick a known video ID from CMS
    cms_path = script_dir.parent / 'output' / 'analytics' / 'Internet_cms_enriched.json'
    with open(cms_path) as f:
        cms = json.load(f)

    # Find a video created before 2024
    test_video = None
    for v in cms:
        created = v.get('created_at', '')[:10]
        if created and created < '2024-01-01':
            test_video = v
            break

    if not test_video:
        print("No pre-2024 video found in CMS")
        return

    video_id = str(test_video['id'])
    video_name = test_video.get('name', 'Unknown')[:50]
    created_at = test_video.get('created_at', '')[:10]
    print(f"\nTest video: {video_id}")
    print(f"  Name: {video_name}")
    print(f"  Created: {created_at}")

    # Make API call
    url = "https://analytics.api.brightcove.com/v1/data"
    params = {
        "accounts": account_id,
        "dimensions": "date",
        "where": f"video=={video_id}",
        "fields": "video_view,video_impression",
        "from": "2024-01-01",
        "to": "2024-12-31",
        "limit": 366,
        "sort": "date"
    }

    headers = {"Authorization": f"Bearer {token}"}

    print(f"\nAPI Request:")
    print(f"  URL: {url}")
    print(f"  Params: {json.dumps(params, indent=2)}")

    import requests
    response = requests.get(
        url,
        headers=headers,
        params=params,
        proxies=secrets.get('proxies', {}),
        timeout=60
    )

    print(f"\nAPI Response:")
    print(f"  Status: {response.status_code}")
    print(f"  Headers: {dict(response.headers)}")

    if response.status_code == 200:
        data = response.json()
        items = data.get('items', [])
        print(f"  Items count: {len(items)}")
        if items:
            print(f"  First item: {json.dumps(items[0], indent=2)}")
            print(f"  Last item: {json.dumps(items[-1], indent=2)}")
        else:
            print("  NO ITEMS RETURNED - this video has no 2024 data")
            print(f"  Full response: {json.dumps(data, indent=2)}")
    else:
        print(f"  Error body: {response.text[:500]}")


if __name__ == "__main__":
    main()
