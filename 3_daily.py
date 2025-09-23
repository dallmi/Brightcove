import requests
import json
import csv
import time
import random
import os
import sys
from base64 import b64encode
from tqdm import tqdm
from datetime import datetime, timedelta
from requests.exceptions import ProxyError, ConnectionError, ReadTimeout

# === Purpose ===
"""
Time to run for 3 years - 2 hrs - Internet Videos filtered down to 2,6k Videos, which were Viewed within last 90 days from last 3 years.
The Purpose of this script is to take the cached cms data, where there were Views within last 90 days.
For this data call the analytics API video by video and date to extract the views.
"""

# bring in secrets
with open("secrets.json") as f:
    SECRETS = json.load(f)

client_id = SECRETS["client_id"]
client_secret = SECRETS["client_secret"]

accounts = {
    "internet": {
        "account_id": "1197194721001",
        "input_file": f"json/internet_cms_cache_2024_2025.json"
    },
    "intranet": {
        "account_id": "4413047246001", 
        "input_file": f"json/intranet_cms_cache_2024_2025.json"
    },
    "neo": {
        "account_id": "5972928207001",
        "input_file": f"json/neo_cms_cache_2024_2025.json"
    },
    "research": {
        "account_id": "3467683096001",
        "input_file": f"json/research_cms_cache_2024_2025.json"
    },
    "research_internal": {
        "account_id": "3731172721001",
        "input_file": f"json/research_internal_cms_cache_2024_2025.json"
    }
}

# === Proxy Settings ===
use_proxies = True
proxies = SECRETS.get("proxies", None) if use_proxies else None

# === UPDATE Time range ===
date_fmt = "%Y-%m-%d"
from_date = "2025-01-01"
to_date = datetime.now().strftime(date_fmt)

from_clean = from_date.replace("-", "_")
to_clean = to_date.replace("-", "_")
master_csv = f"2025/daily_analytics_summary_{from_clean}_to_{to_clean}.csv"
master_csv_research = f"2025/daily_analytics_summary_research_{from_clean}_to_{to_clean}.csv"
checkpoint_file = "checkpoint_log.txt" # Changed from jsonl to avoid windows file lock issues
report_generated_on = datetime.now().strftime(date_fmt)

# === Retry Wrapper ===
def safe_get(url, headers, proxies, max_retries=5, delay=5):
    for attempt in range(max_retries):
        try:
            return requests.get(url, headers=headers, proxies=proxies, timeout=60)
        except (ProxyError, ConnectionError, ReadTimeout) as e:
            print(f"[INFO] Error on attempt {attempt+1}: {e}")
            if attempt < max_retries:
                time.sleep(delay + random.uniform(0, 2))
                delay *= 2
    print(f"[INFO] Max retries exceeded: {url}")
    return None

# === Token Manager ===
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
        auth = b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}
        response = requests.post("https://oauth.brightcove.com/v3/access_token", headers=headers, data=data, proxies=self.proxies)
        response.raise_for_status()
        self.token = response.json().get("access_token")
        self.token_created_at = time.time()
        return self.token

# === API Calls ===
def get_daily_summary(video_id, account_id, auth_manager, start_date):
    fields = [
        "video", "date", "video_impression", "play_rate", "engagement_score",
        "video_engagement_1", "video_engagement_25", "video_engagement_50",
        "video_engagement_75","video_engagement_100", 
        "video_percent_viewed", "video_seconds_viewed", "video_view"
    ]
    token = auth_manager.get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"https://analytics.api.brightcove.com/v1/data"
        f"?accounts={account_id}&dimensions=video,date"
        f"&fields={','.join(fields)}"
        f"&where=video=={video_id}&from={start_date}&to={to_date}&limit=5000"
    )
    response = safe_get(url, headers, proxies)
    return response.json().get("items", []) if response and response.status_code == 200 else []

def get_daily_device_metrics(video_id, account_id, auth_manager, start_date):
    token = auth_manager.get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"https://analytics.api.brightcove.com/v1/data"
        f"?accounts={account_id}&dimensions=video,date,device_type"
        f"&fields=video,date,device_type,video_view"
        f"&where=video=={video_id}&from={start_date}&to={to_date}&limit=10000"
    )
    response = safe_get(url, headers, proxies)
    return response.json().get("items", []) if response and response.status_code == 200 else []

# === Resume from Checkpoint based on last date per video ===
def load_checkpoint():
    last_date_map = {}  # (account_id,video_id) -> max_date_str)
    checkpoint_rows = []
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    row = json.loads(line)
                    acc = row.get("account_id")
                    video_id = row.get("video_id")
                    date = row.get("date")
                    key = (acc, video_id)
                    # keep max date
                    if key not in last_date_map or date > last_date_map[key]:
                        last_date_map[key] = date
                    checkpoint_rows.append(row)
                except:
                    continue
    return last_date_map, checkpoint_rows

# === MAIN ===
def main():
    auth_manager = BrightcoveAuthManager(client_id, client_secret, proxies)
    last_date_map, checkpoint_rows = load_checkpoint()
    
    with open(checkpoint_file, "a", encoding="utf-8") as chk:
        for channel, info in accounts.items():
            account_id = info["account_id"]
            input_file = info["input_file"]
            tqdm.write(f"\nProcessing channel: {channel}")
            
            with open(input_file) as f:
                videos = json.load(f)
                
            video_loop = tqdm(videos, desc=f"{channel} videos", unit="video", dynamic_ncols=True)
            start_time = time.time()
            
            for idx, video in enumerate(video_loop):
                video_id = video.get("id")
                key = (account_id, video_id)
                last_dt = last_date_map.get(key, from_date)
                
                # compute next date depending on last date found for a specific video, account id combination in logfile
                next_dt = (datetime.strptime(last_dt, date_fmt) + timedelta(days=1)).strftime(date_fmt)
                
                # skip if no new dates
                if next_dt > to_date:
                    continue
                name = video.get("name", "")
                duration = video.get("duration", 0)
                
                # fetch only new range
                summary_data = get_daily_summary(video_id, account_id, auth_manager, next_dt)
                device_data = get_daily_device_metrics(video_id, account_id, auth_manager, next_dt)
                
                # Device breakdown map
                device_map = {}
                for item in device_data:
                    date = item.get("date")
                    device = item.get("device_type", "other").lower()
                    views = item.get("video_view", 0)
                    if date not in device_map:
                        device_map[date] = {
                            "views_desktop": 0, "views_mobile": 0,
                            "views_tablet": 0, "views_other": 0
                        }
                    key = f"views_{device}" if device in ["desktop", "mobile", "tablet"] else "views_other"
                    device_map[date][key] += views
                
                for item in summary_data:
                    date = item.get("date")
                    key = (account_id, video_id, date)
                    # skip if somehow present
                    if date <= last_date_map.get(key, "0000-00-00"):
                        continue
                    
                    row = {
                        "channel": channel,
                        "account_id": account_id,
                        "video_id": video_id,
                        "name": name,
                        "date": date,
                        "video_view": item.get("video_view", 0),
                        "video_impression": item.get("video_impression", 0),
                        "play_rate": item.get("play_rate", 0),
                        "engagement_score": item.get("engagement_score", 0),
                        "video_engagement_1": item.get("video_engagement_1", 0),
                        "video_engagement_25": item.get("video_engagement_25", 0),
                        "video_engagement_50": item.get("video_engagement_50", 0),
                        "video_engagement_75": item.get("video_engagement_75", 0),
                        "video_engagement_100": item.get("video_engagement_100", 0),
                        "video_percent_viewed": item.get("video_percent_viewed", 0),
                        "video_seconds_viewed": item.get("video_seconds_viewed", 0),
                        "views_desktop": 0,
                        "views_mobile": 0,
                        "views_tablet": 0,
                        "views_other": 0,
                        "created_at": video.get("created_at", ""),
                        "published_at": video.get("published_at", ""),
                        "original_filename": video.get("original_filename", ""),
                        "created_by": video.get("created_by", ""),
                        "tags": ",".join(video.get("tags", [])),
                        "reference_id": video.get("reference_id", ""),
                        "video_duration": duration,
                        "report_generated_on": report_generated_on
                    }
                    
                    custom = video.get("custom_fields", {})
                    row["video_content_type"] = custom.get("video_content_type", "")
                    row["video_length"] = custom.get("video_length", "")
                    row["video_category"] = custom.get("video_category", "")
                    row["country"] = custom.get("country", "")
                    row["language"] = custom.get("language", "")
                    row["business_unit"] = custom.get("business_unit", "")
                    
                    if date in device_map:
                        row.update(device_map[date])
                    
                    chk.write(json.dumps(row) + "\n")
                    chk.flush()
                    checkpoint_rows.append(row)
                    
                    # update last_date_map
                    last_date_map[key] = date
                
                # ETA update
                elapsed = time.time() - start_time
                avg_time = elapsed / (idx + 1)
                remaining = avg_time * (len(videos) - idx - 1)
                video_loop.set_postfix_str(f"ETA: {int(remaining // 60)}m {int(remaining % 60)}s")
    
    csv_fields = [
        "channel", "account_id", "video_id", "name", "date",
        "video_view",
        "views_desktop", "views_mobile", "views_tablet", "views_other", 
        "video_impression", "play_rate", "engagement_score",
        "video_engagement_1",
        "video_engagement_25",
        "video_engagement_50", "video_engagement_75",
        "video_engagement_100", "video_percent_viewed", "video_seconds_viewed",
        "created_at", "published_at", "original_filename", "created_by",
        "video_content_type", "video_length", "video_duration", "video_category",
        "country", "language",
        "business_unit", "tags", "reference_id",
        "report_generated_on"
    ]
    
    with open(master_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in checkpoint_rows:
            if row["channel"] in ["internet", "intranet"]:
                writer.writerow(row)
    print(f"Final output written to: {master_csv}")
    
    with open(master_csv_research, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()
        for row in checkpoint_rows:
            if row["channel"] in ["neo", "research", "research_internal"]:
                writer.writerow(row)
    print(f"Final output written to: {master_csv_research}")

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    main()