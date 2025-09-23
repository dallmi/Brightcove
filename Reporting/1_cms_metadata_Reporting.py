import requests
from base64 import b64encode
import json
import time
import csv
from tqdm import tqdm
from datetime import datetime

# Purpose
"""
Time to run for 3 years - 5 mins
The Purpose of this script is to fetch cms/meta data (video ids) for all accounts within the given years to fetch without date granularity.
The API calls are happening in batches not looking at daily break-downs or custom fields, making this script to finish within few mins.
It reduces the video_ids to only the ones being published and active in order to reduce the Api calls made in VideoCache > break down by days and custom fields.
"""

# bring in secrets
with open("secrets.json") as f:
    SECRETS = json.load(f)

client_id = SECRETS["client_id"]
client_secret = SECRETS["client_secret"]


# === Proxy Settings ===
use_proxies = True
proxies = SECRETS.get("proxies", None) if use_proxies else None


# Local file output for selected years
years_to_fetch = ["2024", "2025"]

# === Output Files ===
year_suffix = "_".join(years_to_fetch)

accounts = {
    "Internet": {
        "account_id": "1197194721001",
        "json_output_file": f'json/internet_cms_{year_suffix}.json',
        "csv_output_file": f'csv/internet_cms_{year_suffix}.csv'
    },
    "Intranet": {
        "account_id": "4413047246001",
        "json_output_file": f'json/intranet_cms_{year_suffix}.json',
        "csv_output_file": f'csv/intranet_cms_{year_suffix}.csv'
    },
    "neo": {
        "account_id": "5972928207001",
        "json_output_file": f'json/neo_cms_{year_suffix}.json',
        "csv_output_file": f'csv/neo_cms_{year_suffix}.csv'
    },
    "research": {
        "account_id": "3467683096001",
        "json_output_file": f'json/research_cms_{year_suffix}.json',
        "csv_output_file": f'csv/research_cms_{year_suffix}.csv'
    },
    "research_internal": {
        "account_id": "3731172721001",
        "json_output_file": f'json/research_internal_cms_{year_suffix}.json',
        "csv_output_file": f'csv/research_internal_cms_{year_suffix}.csv'
    }
}


# === AUTHENTICATION ===
class BrightcoveAuthManager:
    def __init__(self, client_id, client_secret, proxies=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_created_at = 0
        self.token_expires_in = 300
        self.proxies = proxies
    
    def get_token(self):
        if self.token and (time.time() - self.token_created_at) < self.token_expires_in - 30:
            return self.token
        return self.refresh_token()
    
    def refresh_token(self):
        print("[INFO] Refreshing access token...")
        auth = b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        response = requests.post(
            "https://oauth.brightcove.com/v3/access_token", 
            headers=headers, data=data, proxies=self.proxies
        )
        response.raise_for_status()
        self.token = response.json().get('access_token')
        self.token_created_at = time.time()
        print("[INFO] Access token refreshed.")
        return self.token

# === FETCH CMS VIDEO LIST ===
def fetch_all_videos(token, account_id):
    headers = {"Authorization": f"Bearer {token}"}
    limit = 100
    offset = 0
    all_videos = []
    tqdm.write(f"[INFO] Fetching videos from CMS API for account {account_id}...")
    
    while True:
        url = f"https://cms.api.brightcove.com/v1/accounts/{account_id}/videos?limit={limit}&offset={offset}&sort=created_at"
        response = requests.get(url, headers=headers, proxies=proxies)
        if response.status_code != 200:
            tqdm.write(f"[ERROR] Failed to fetch videos: {response.status_code} {response.text}")
            break

        batch = response.json()
        if not batch:
            break

        all_videos.extend(batch)
        offset += limit

        tqdm.write(f"[INFO] Retrieved {len(batch)} videos (total: {len(all_videos)})")
    
    return all_videos

# === MAIN ===
def main():
    
    for channel, details in accounts.items():
        auth_manager = BrightcoveAuthManager(client_id, client_secret, proxies=proxies)
        token = auth_manager.get_token()
        account_id = details["account_id"]
        json_output_file = details["json_output_file"]
        csv_output_file = details["csv_output_file"]
        
        print(f"Running for {channel} with account_id {account_id}")
        
        all_videos = fetch_all_videos(token, account_id)

        # Filter by created_at year prefix
        filtered_videos = [
            video for video in all_videos 
            if any(video.get("created_at", "").startswith(year) for year in years_to_fetch)
            and video.get("state","").upper() == "ACTIVE"
            and video.get("published_at") is not None
        ]
        
        tqdm.write(f"[INFO] Videos created in {", ".join(years_to_fetch)}: {len(filtered_videos)}")
        
        # Write to JSON
        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(all_videos, f, indent=2)
            tqdm.write(f"[INFO] JSON written: {json_output_file}")
        
        # Write to CSV
        csv_fields = [
            "id", "name", "created_at", "published_at", "original_filename", "created_by",
            "duration", "state", "reference_id", "tags"
        ]
        
        custom_fields = [
            "video_content_type", "video_length", "video_category",
            "country", "language", "business_unit"
        ]
        
        with open(csv_output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields + custom_fields)
            writer.writeheader()
            for video in filtered_videos:
                row = {field: video.get(field, "") for field in csv_fields}
                row["tags"] = ",".join(video.get("tags", []))
                for cf in custom_fields:
                    row[cf] = video.get("custom_fields", {}).get(cf, "")
                writer.writerow(row)
            
            tqdm.write(f"[INFO] CSV written: {csv_output_file}")

if __name__ == "__main__":
    main()