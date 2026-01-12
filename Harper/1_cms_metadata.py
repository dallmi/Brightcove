import requests
from base64 import b64encode
import json
import time
import csv
from tqdm import tqdm
from datetime import datetime

# Purpose 
"""
Time to run is around 8 mins
The Purpose of this script is to fetch cms/meta data (video ids) for all accounts without date restrictions.
The API calls are happening in batches not looking at daily break-downs of custom fields, making this script finishin within few mins.
API calls made in LastViewed --> break down by days and custom columns
"""

#bring in secrets
with open('secrets.json') as f:
    Secrets = json.load(f)

client_id = Secrets['client_id']
client_secret = Secrets['client_secret']


# proxy settings
use_proxies = True
proxies = Secrets.get('proxies', None) if use_proxies else None


accounts = {
    "Internet": {
        "account_id": "1197194721001",
        "json_output_file": f'json/internet_cms_metadata.json',
        "csv_output_file": f'csv/internet_cms_metadata.csv',
    },
    "Intranet": {
        "account_id": "4413047246001",
        "json_output_file": f'json/intranet_cms_metadata.json',
        "csv_output_file": f'csv/intranet_cms_metadata.csv',
    },
    "neo": {
        "account_id": "5972928207001",
        "json_output_file": f'json/neo_cms_metadata.json',
        "csv_output_file": f'csv/neo_cms_metadata.csv',
    },
    "research": {
        "account_id": "3467683096001",
        "json_output_file": f'json/research_cms_metadata.json',
        "csv_output_file": f'csv/research_cms_metadata.csv',
    },
    "research_internal": {
        "account_id": "3731172721001",
        "json_output_file": f'json/research_internal_cms_metadata.json',
        "csv_output_file": f'csv/research_internal_cms_metadata.csv',
    },
    "impact": {
        "account_id": "968049871001",
        "json_output_file": f'json/impact_cms_metadata.json',
        "csv_output_file": f'csv/impact_cms_metadata.csv',
    },
    "circleone": {
        "account_id": "6283605170001",
        "json_output_file": f'json/circleone_cms_metadata.json',
        "csv_output_file": f'csv/circleone_cms_metadata.csv',
    },
    "digital_networks_events": {
        "account_id": "4631489639001",
        "json_output_file": f'json/digital_networks_events_cms_metadata.json',
        "csv_output_file": f'csv/digital_networks_events_cms_metadata.csv',
    },
    "fa_web": {
        "account_id": "807049819001",
        "json_output_file": f'json/fa_web_cms_metadata.json',
        "csv_output_file": f'csv/fa_web_cms_metadata.csv',
    },
    "SuMiTrust": {
        "account_id": "5653786046001",
        "json_output_file": f'json/Sumi_Trust_cms_metadata.json',
        "csv_output_file": f'csv/Sumi_Trust_cms_metadata.csv',
    },
    "MyWay": {
        "account_id": "6300219615001",
        "json_output_file": f'json/MyWay_cms_metadata.json',
        "csv_output_file": f'csv/MyWay_cms_metadata.csv',
    },
}


# === Authentication ===
class BrightcoveAuthManager:
    def __init__(self, client_id, client_secret, proxies=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_created_at = 0
        self.token_expires_in = 300  # Default to 300 seconds
        self.proxies = proxies
    
    def get_token(self):
        if self.token and (time.time() - self.token_created_at) < self.token_expires_in - 30:
            return self.token  # Return existing token if not expired (with a 30-second buffer)
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
def fetch_all_videos(auth_manager, account_id, proxies=None):
    
    limit = 100
    offset = 0
    all_videos = []
    
    #get total for ETA
    total = requests.get(
        f"https://cms.api.brightcove.com/v1/accounts/{account_id}/counts/videos",
        headers={"Authorization": f"Bearer {auth_manager.get_token()}"},
        proxies=proxies
    ).json()["count"]

    #give tqdm the total so it can show % ETA
    pbar = tqdm(total=total, unit="videos", desc=f"Account {account_id}")

    while True:
        token = auth_manager.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(
            f"https://cms.api.brightcove.com/v1/accounts/{account_id}/videos",
            headers=headers,
            params={
                "limit": limit, 
                "offset": offset,
                "sort": "created_at"
            },
            proxies=proxies
        )
        resp.raise_for_status()

        batch = resp.json()
        if not batch:
            break
        
        all_videos.extend(batch)

        #tell tqdm how many you just got
        pbar.update(len(batch))

        offset += limit

    pbar.close()
    return all_videos



# === MAIN ===
def main():
    auth_manager = BrightcoveAuthManager(client_id, client_secret, proxies=proxies)


    for channel, details in accounts.items():
        account_id = details["account_id"]
        json_output_file = details["json_output_file"]
        csv_output_file = details["csv_output_file"]

        print(f"Running for {channel} with account_id {account_id}")
        
        all_videos = fetch_all_videos(auth_manager, account_id, proxies=proxies)

        tqdm.write(f"[INFO] Videos created: {len(all_videos)}")


        with open(json_output_file, 'w', encoding='utf-8') as f:
            json.dump(all_videos, f, indent=2)
            tqdm.write(f"[INFO] JSON written: {json_output_file}")
        
        # Write to CSV
        csv_fields = [
            "account_id", "id", "name", "original_filename","description", "updated_at", "created_at", "published_at",
            "created_by", "ad_keys", "clip_source_video_id", "complete", "cue_points", "delivery_type", "digital_master_id",
            "duration", "economics", "folder_id", "geo", "has_digital_master", "images", "link", "long_description", "projection", 
            "reference_id", "schedule", "sharing", "state", "tags", "text_tracks", "transcripts", "updated_by", "playback_rights_id",
            "ingestion_profile_id"
        ]
        # Dynamically find all custom fields
        custom_fields = [
            "video_content_type", "relatedlinkname", "relatedlink", "country", "language",
            "business_unit", "video_category", "video_length", "video_owner_email", "1a_comms_sign_off",
            "1b_comms_sign_off_approver", "2a_data_classification_disclaimer", "3a_records_management_disclaimer",
            "4a_archiving_disclaimer_comms_branding", "4b_unique_sharepoint_id"
        ]

        with open(csv_output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields + custom_fields)
            writer.writeheader()
            for video in all_videos:
                row = {field: video.get(field, "") for field in csv_fields}
                row["tags"] = ",".join(video.get("tags", []))
                for cf in custom_fields:
                    row[cf] = video.get("custom_fields", {}).get(cf, "")
                writer.writerow(row)

            tqdm.write(f"[INFO] CSV written: {csv_output_file}")

if __name__ == "__main__":
    main()