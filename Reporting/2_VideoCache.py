from base64 import b64encode
import time
import requests
import pandas as pd
if not hasattr(pd, 'Panel'):
    class DummyPanel:
        def __init__(self, *args, **kwargs):
            raise NotImplementedError("pd.Panel is deprecated and not available in this pandas version.")
    pd.Panel = DummyPanel
from datetime import datetime, timedelta, date, timezone
import json
from tqdm import tqdm
import os

# === Purpose ===
"""
Time to run for 3 years - 3 mins
The Purpose of this script is to take the published and ACTIVE cms data and break it down by day in order to calculate the LAST VIEWED DATE.
LAST_VIEWED_DATE is used to filter the data further down to only the video ids that had any views within last 90 days.
This will reduce the amount of Analytics API calls to be made in daily script, which is calling the API for each video id and date to extract the analytics data.
"""

# bring in secrets
with open("secrets.json") as f:
    SECRETS = json.load(f)

client_id = SECRETS["client_id"]
client_secret = SECRETS["client_secret"]

# === MULTI-ACCOUNT CONFIG ===
ACCOUNTS = {
    "internet": {
        "account_id": "1197194721001",
        "CMS_METADATA_PATH": f'json/internet_cms_2024_2025.json',
        "OUTPUT_PATH": f"csv/internet_cms_cache_2024_2025.csv",
        "OUTPUT_JSON": f"json/internet_cms_cache_2024_2025.json"
    },
    "intranet": {
        "account_id": "4413047246001",
        "CMS_METADATA_PATH": f"json/intranet_cms_2024_2025.json",
        "OUTPUT_PATH": f"csv/intranet_cms_cache_2024_2025.csv",
        "OUTPUT_JSON": f"json/intranet_cms_cache_2024_2025.json"
    },
    "neo": {
        "account_id": "5972928207001",
        "CMS_METADATA_PATH": f"json/neo_cms_2024_2025.json",
        "OUTPUT_PATH": f"csv/neo_cms_cache_2024_2025.csv",
        "OUTPUT_JSON": f"json/neo_cms_cache_2024_2025.json"
    },
    "research": {
        "account_id": "3467683096001",
        "CMS_METADATA_PATH": f"json/research_cms_2024_2025.json",
        "OUTPUT_PATH": f"csv/research_cms_cache_2024_2025.csv",
        "OUTPUT_JSON": f"json/research_cms_cache_2024_2025.json"
    },
    "research_internal": {
        "account_id": "3731172721001",
        "CMS_METADATA_PATH": f"json/research_internal_cms_2024_2025.json",
        "OUTPUT_PATH": f"csv/research_internal_cms_cache_2024_2025.csv",
        "OUTPUT_JSON": f"json/research_internal_cms_cache_2024_2025.json"
    }
    # Add more accounts as needed
}

# === DATES ===
# now = datetime.utcnow()
DAYS_BACK = 90
# start_date = now - timedelta(days=DAYS_BACK)
# FROM_TIME = start_date.strftime('%Y-%m-%d')
# TO_TIME = now.strftime('%Y-%m-%d')


# === Proxy Settings ===
use_proxies = True
proxies = SECRETS.get("proxies", None) if use_proxies else None

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
        response = requests.post(
            "https://oauth.brightcove.com/v3/access_token",
            headers=headers, data=data, proxies=self.proxies
        )
        response.raise_for_status()
        self.token = response.json().get("access_token")
        self.token_created_at = time.time()
        return self.token

# === FETCH ANALYTICS WITH PAGINATION + CACHING ===
def fetch_analytics(account_id, access_token, from_time, to_time, page_limit=10000, cache_prefix="analytics_cache"):
    all_items = []
    offset = 0
    headers = {"Authorization": f"Bearer {access_token}"}
    cache_file = f"{cache_prefix}_{from_time}_to_{to_time}.json"
    
    if os.path.exists(cache_file):
        print(f"[INFO] Loading cached data from {cache_file}...")
        with open(cache_file, "r") as f:
            all_items = json.load(f)
        offset = len(all_items)
        print(f"[INFO] Resuming from offset {offset} (already had {len(all_items)} items)")
    
    pbar = tqdm(desc="[INFO] Fetching Brightcove pages", unit="rows", initial=offset)
    
    while True:
        url = (
            f"https://analytics.api.brightcove.com/v1/data"
            f"?accounts={account_id}"
            f"&dimensions=video,date"
            f"&from={from_time}"
            f"&to={to_time}"
            f"&limit={page_limit}"
            f"&offset={offset}"
        )
        #print(response.txt)
        print(f"[INFO] [{time.strftime('%H:%M:%S')}] Fetching offset {offset}...")
        try:
            response = requests.get(url, headers=headers, proxies=proxies)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[INFO] Error fetching data at offset {offset}: {e}")
            break
        
        data = response.json()
        items = data.get("items", [])
        
        if not items:
            print("[INFO] No more items returned.")
            break
        
        all_items.extend(items)
        offset += len(items)
        pbar.update(len(items))
        # Save cache after each batch
        with open(cache_file, "w") as f:
            json.dump(all_items, f, indent=2)

        if len(items) < page_limit:
            break
        time.sleep(0.1)
    
    pbar.close()
    print(f"[INFO] Fetched total {len(all_items)} video-date rows across pages.")
    return {"items": all_items}


# === BUILD LAST VIEWED CACHE ===
def build_last_view_cache(items):
    if not items:
        print("[INFO] No items to process.")
        return pd.DataFrame()
    
    df = pd.DataFrame(items)
    if 'video' not in df.columns or 'date' not in df.columns:
        print("[INFO] Missing 'video' or 'date' columns")
        return pd.DataFrame()

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    min_date = df['date'].min()
    max_date = df['date'].max()
    print(f"[INFO] Data covers from {min_date.date()} to {max_date.date()} ({(max_date - min_date).days} days)")
    
    tqdm.pandas(desc="[INFO] Finding last viewed per video")
    last_view_df = df.groupby("video")['date'].progress_apply(lambda x: x.max()).reset_index()
    last_view_df.rename(columns={'date': 'last_viewed_date'}, inplace=True)
    return last_view_df

# === LOAD + FILTER CMS METADATA ===
def load_cms_metadata(json_path):
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"CMS metadata file not found: {json_path}")
    
    with open(json_path, "r") as f:
        cms_data = json.load(f)
    
    cms_df = pd.DataFrame(cms_data)
    cms_df.rename(columns={'id': 'video'}, inplace=True)

    cms_df['state'] = cms_df['state'].fillna('').astype(str)
    if "publishing_state" not in cms_df.columns:
        cms_df['publishing_state'] = 'published'
    else:
        cms_df['publishing_state'] = cms_df['publishing_state'].fillna('').astype(str)
    
    filtered_df = cms_df[
        (cms_df['state'].str.upper() == 'ACTIVE') &
        (cms_df['publishing_state'].str.lower() == 'published')
    ]
    
    print(f"[INFO] Filtered to {len(filtered_df)} ACTIVE + published videos.")
    return filtered_df

# === MAIN LOOP FOR ALL ACCOUNTS ===
def main():
    # now = datetime.utcnow()
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(DAYS_BACK)
    from_time = start_date.strftime('%Y-%m-%d')
    to_time = now.strftime('%Y-%m-%d')
    # from_time = "2023-01-01"
    # to_time = "2024-12-31"
    # print(from_time)
    # print("="*50)
    # print(to_time)
    
    token_mgr = BrightcoveAuthManager(client_id, client_secret, proxies=proxies)
    

    for name, config in ACCOUNTS.items():
        print(f"\n[INFO] Processing account: {name} ({config['account_id']})")
        token = token_mgr.get_token()  # refresh logic now runs each iteration
        
        try:
            result = fetch_analytics(
                account_id=config["account_id"],
                access_token=token,
                from_time=from_time,
                to_time=to_time,
                page_limit=10000,
                cache_prefix=f"analytics_cache_{name}"
            )
            
            items = result.get("items", [])
            if not items:
                print(f"[INFO] No analytics items for {name}")
                continue
            
            last_view_df = build_last_view_cache(items)
            cms_df = load_cms_metadata(config["CMS_METADATA_PATH"])
            
            merged_df = pd.merge(last_view_df, cms_df, on="video", how="inner")
            merged_df["channel"] = name
            merged_df.rename(columns={'video': 'id'}, inplace=True)
            

            output_path = config["OUTPUT_PATH"]
            output_json = config["OUTPUT_JSON"]
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            merged_df.to_csv(output_path, index=False)
            merged_df.to_json(output_json, orient='records', date_format='iso')
            print(f"[INFO] Saved {len(merged_df)} rows to {output_path}")
            print(f"[INFO] Saved {len(merged_df)} rows to {output_json}")

        except Exception as e:
            print(f"[INFO] Failed for account {name}: {e}")

if __name__ == "__main__":
    main()