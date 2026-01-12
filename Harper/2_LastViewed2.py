from base64 import b64encode
import time
import json
import csv
import sys
import requests
from tqdm import tqdm
from datetime import datetime, timedelta
from requests.exceptions import HTTPError
import os


#from brightcove_auth import Brightcove AuthManager

# -- CONFIGURE YOUR ACCOUNTS ----------
# Fill in each entry with your real account name, ID, CMS metadata input path,
# and desired JSON/CSV output paths.

#brin in secrets
with open('secrets.json') as f:
    Secrets = json.load(f)

client_id = Secrets['client_id']
client_secret = Secrets['client_secret']

# proxy settings
use_proxies = True
proxies = Secrets.get('proxies', None) if use_proxies else None

# === MULTI-ACCOUNT CONFIG===
ACCOUNTS = {
    "Intranet": {
        "account_id": "4413047246001",
        "cms_metadata_in": f'json/intranet_cms_metadata.json',
        "output_csv": f'csv/intranet_cms.csv',
        "output_json": f'json/intranet_cms.json',
    },
    "neo": {
        "account_id": "5972928207001",
        "cms_metadata_in": f'json/neo_cms_metadata.json',
        "output_csv": f'csv/neo_cms.csv',
        "output_json": f'json/neo_cms.json',
    },
    "research": {
        "account_id": "3467683096001",
        "cms_metadata_in": f'json/research_cms_metadata.json',
        "output_csv": f'csv/research_cms.csv',
        "output_json": f'json/research_cms.json',
    },
    "research_internal": {
        "account_id": "3731172721001",
        "cms_metadata_in": f'json/research_internal_cms_metadata.json',
        "output_csv": f'csv/research_internal_cms.csv',
        "output_json": f'json/research_internal_cms.json',
    },
    "impact": {
        "account_id": "968049871001",
        "cms_metadata_in": f'json/impact_cms_metadata.json',
        "output_csv": f'csv/impact_cms.csv',
        "output_json": f'json/impact_cms.json',
    },
    "circleone": {
        "account_id": "6283605170001",
        "cms_metadata_in": f'json/circleone_cms_metadata.json',
        "output_csv": f'csv/circleone_cms.csv',
        "output_json": f'json/circleone_cms.json',
    },
    "digital_networks_events": {
        "account_id": "4631489639001",
        "cms_metadata_in": f'json/digital_networks_events_cms_metadata.json',
        "output_csv": f'csv/digital_networks_events_cms.csv',
        "output_json": f'json/digital_networks_events_cms.json',
    },
    "fa_web": {
        "account_id": "807049819001",
        "cms_metadata_in": f'json/fa_web_cms_metadata.json',
        "output_csv": f'csv/fa_web_cms.csv',
        "output_json": f'json/fa_web_cms.json',
    },
    "SuMiTrust": {
        "account_id": "5653786046001",
        "cms_metadata_in": f'json/Sumi_Trust_cms_metadata.json',
        "output_csv": f'csv/Sumi_Trust_cms.csv',
        "output_json": f'json/Sumi_Trust_cms.json',
    },
    #"SeniorMgmtInternal": {
    #    "account_id": "4571860231001",
    #    "csv_output_file": f'csv/SeniorMgmtInternal_cms.csv',
    #    "output_json": f'json/SeniorMgmtInternal_cms.json',
    #},
    "MyWay": {
        "account_id": "6300219615001",
        "cms_metadata_in": f'json/MyWay_cms_metadata.json',
        "output_csv": f'csv/MyWay_cms.csv',
        "output_json": f'json/MyWay_cms.json',
    },
    "Internet": {
        "account_id": "1197194721001",
        "cms_metadata_in": f'json/internet_cms_metadata.json',
        "output_csv": f'csv/internet_cms.csv',
        "output_json": f'json/internet_cms.json',
    },
}


# === Token Manager ===
class BrightcoveAuthManager:
    def __init__(self, client_id, client_secret, proxies=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_created_at = 0
        self.token_expires_in = 360
        self.proxies = proxies

    def get_token(self):
        if self.token and (time.time() - self.token_created_at) < self.token_expires_in - 30:
            return self.token
        return self.refresh_token()

    def refresh_token(self):
        print("[INFO] Refreshing token...")
        auth = b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}
        response = requests.post(
            "https://oauth.brightcove.com/v3/access_token",
            headers=headers, data=data, proxies=self.proxies
        )
        response.raise_for_status()
        self.token = response.json().get('access_token')
        self.token_created_at = time.time()
        print("[INFO] Token refreshed.")
        return self.token

# ----- END CONFIG-------------------------------------

# reusable functions

def get_date_bounds(auth_manager, account_id, proxies=None):
    """
    Returns a tuple with (first_date, last_date) of any views for the account.
    """
    url = "https://analytics.api.brightcove.com/v1/data"
    base_params = {
        "accounts": account_id,
        "dimensions":   "date",
        "fields":       "video_view",
        "from":         "alltime",
        "to":           "now",
        "limit":        1
    }
    #headers = lambda: {"Authorization": f"Bearer {auth_manager.get_token()}"}

    # get token
    token = auth_manager.get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # earliest date
    p_asc = {**base_params, "sort": "date"}
    resp = requests.get(url, headers=headers, params=p_asc, proxies=proxies)
    resp.raise_for_status()
    first = resp.json()["items"][0]["date"]

    # latest date
    p_desc = {**base_params, "sort": "-date"}
    resp = requests.get(url, headers=headers, params=p_desc, proxies=proxies)
    resp.raise_for_status()
    last = resp.json()["items"][0]["date"]

    return first, last


def make_year_windows(start_iso, end_iso):
    """
    Split the range from start_iso to end_iso into half-year windows (Jan-Jun, Jul-Dec).
    The final window will use "now" if end_iso is the current year.
    """
    start_dt = datetime.strptime(start_iso, "%Y-%m-%d")
    end_dt = datetime.strptime(end_iso, "%Y-%m-%d")
    windows = []
    year = start_dt.year
    #iterate year by year
    while year <= end_dt.year:
        # First half of the year
        first_start = datetime(year, 1, 1)
        first_end = datetime(year, 6, 30)
        # second half of the year
        second_start = datetime(year, 7, 1)
        second_end = datetime(year, 12, 31)

        #first half: only if overlapping with start_dt, end_dt
        frm_dt = max(first_start, start_dt)
        to_dt = min(first_end, end_dt)
        if frm_dt <= to_dt:
            frm = frm_dt.strftime("%Y-%m-%d")
            to = to_dt.strftime("%Y-%m-%d") if (to_dt < end_dt or year < end_dt.year) else "now"
            windows.append((frm, to))

        #second half: only if overlapping with start_dt, end_dt
        frm_dt = max(second_start, start_dt)
        to_dt = min(second_end, end_dt)
        if frm_dt <= to_dt:
            frm = frm_dt.strftime("%Y-%m-%d")
            to = to_dt.strftime("%Y-%m-%d") if (to_dt < end_dt or year < end_dt.year) else "now"
            windows.append((frm, to))

        year += 1
    return windows


def fetch_slice(auth_manager, account_id, frm, to, limit=10000, proxies=None):
    """
    Fetch all (video, date, video_view) rows for [frm, to], paging via offset.
    Drops 'reconciled' if to == 'now'.
    """
    url = "https://analytics.api.brightcove.com/v1/data"

    params = {
        "accounts": account_id,
        "dimensions":   "video,date",
        "fields":       "video_view",
        "sort":         "-date",
        "from":         frm,
        "to":           to,
        "limit":        limit,
        "offset":       0,
        "reconciled":   "true"
    }

    #if this slice goes up to now, drop the recciled flag so we only request live data
    if to == "now" and 'reconciled' in params:
        del params['reconciled']

    rows = []
    while True:
        token = auth_manager.get_token() #will refresh if we're within 30s of expiry
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers, params=params, proxies=proxies)
        try:
            r.raise_for_status()
        except HTTPError:
            print("\n--- Brightcove API error ---")
            print("Slice:            ", frm, "-->", to)
            print("Request URL:    ", r.request.url)
            print("Params: ", params)
            print("Headers: ", {k: headers[k] for k in headers})
            print("Proxy used: ", proxies)
            print("Status code:   ", r.status_code)
            print("Response text: ")
            print(r.text)
            print("--- TRACEBACK ---\n")
            #TRACEBACK.print_exc()
            raise

        items = r.json().get("items", [])
        if not items:
            break
        rows.extend(items)
        params["offset"] += len(items)
    return rows


def merge_last_views(last_map, slice_rows):
    """
    Update last_map in place with max date per video from slice_rows.
    """
    for item in slice_rows:
        vid = item.get("video")
        dt = item.get("date")
        if item.get("video_view", 0) > 0:
            if vid not in last_map or dt > last_map[vid]:
                last_map[vid] = dt

def split_window(frm, to):
    # Split window in half, expects dates as YYYY-MM-DD
    start = datetime.strptime(frm, "%Y-%m-%d")
    end = datetime.strptime(to, "%Y-%m-%d")
    mid = start + (end - start) / 2
    mid_date = mid.strftime("%Y-%m-%d")
    next_day = (datetime.strptime(mid_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return [(frm, mid_date), (next_day, to)]

def enrich_metadata(cms_path, last_map):
    """
    Add at_last_viewed and unnest custom_fields as cf_<key> to each record.
    """
    with open(cms_path, "r", encoding="utf-8") as f:
        videos = json.load(f)

    for v in videos:
        # rename last_view_date to dt_last_viewed
        v["dt_last_viewed"] = last_map.get(v.get("id"))
        # unnest custom_fields
        cf = v.pop("custom_fields", {}) or {}
        for k, val in cf.items():
            v[f"cf_{k}"] = val

    return videos


def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def save_csv(data, path):
    """
    Write CSV with fixed column order plus dynamic cf_ columns.
    """
    if not data:
        return
    fixed_fields = [
        'account_id', 'id', 'name', 'original_filename', 'description',
        'dt_last_viewed', 'updated_at', 'created_at', 'published_at',
        'created_by', 'ad_keys', 'clip_source_video_id', 'complete', 'cue_points',
        'delivery_type', 'digital_master_id','duration', 'economics', 'folder_id',
        'geo', 'has_digital_master', 'images', 'link', 'long_description',
        'projection', 'reference_id', 'schedule', 'sharing', 'state', 'tags',
        'text_tracks', 'transcripts', 'updated_by', 'playback_rights_id',
        'ingestion_profile_id'
    ]
    # discover cf_ fields
    cf_fields = []
    for row in data:
        for k in row:
            if k.startswith('cf_') and k not in cf_fields:
                cf_fields.append(k)
    fieldnames = fixed_fields + cf_fields

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            out = {k: row.get(k, "") for k in fieldnames}
            writer.writerow(out)


#---- MAIN WITH CHECKPOINTING ----
def main():
    auth_manager = BrightcoveAuthManager(client_id, client_secret, proxies)

    for name, cfg in ACCOUNTS.items():
        print(f"Processing: {name}")
        # 1) check if checkpoint exists and load to avoid unnecessary API calls
        checkpoint_file = f"{name}_checkpoint.json"
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                chk = json.load(f)
            last_map = chk.get("last_map", {})
            windows_done = set(chk.get("windows_done", []))
            print(f"Resuming from checkpoint, {len(windows_done)} windows already done.")
        else:
            last_map = {}
            windows_done = set()

        # 2) find full date bounds
        first, last = get_date_bounds(auth_manager, cfg['account_id'], proxies)
        windows = make_year_windows(first, last)

        # 3) fetch rows via API call using a single while loop with manual index control
        i = 0
        pbar = tqdm(total=len(windows), desc=f"Fetching slices for {name}")

        while i < len(windows):
            frm, to = windows[i]
            key = f"{frm}_{to}"

            # Skip already-completed windows
            if key in windows_done:
                i += 1
                pbar.update(1)
                continue

            max_retries = 3
            backoff = 5
            success = False

            for attempt in range(1, max_retries + 1):
                try:
                    slice_rows = fetch_slice(
                        auth_manager, cfg['account_id'], frm, to,
                        proxies=proxies
                    )
                    merge_last_views(last_map, slice_rows)
                    windows_done.add(key)
                    # update checkpoint
                    with open(checkpoint_file, "w", encoding="utf-8") as f:
                        json.dump({
                            "last_map": last_map,
                            "windows_done": list(windows_done)
                        }, f)
                    success = True
                    break
                except Exception as e:
                    print(f"Attempt {attempt}/{max_retries} failed for window {key}: {e}")
                    if attempt < max_retries:
                        print(f"Retrying in {backoff} seconds...")
                        time.sleep(backoff)
                        backoff *= 2

            if success:
                i += 1
                pbar.update(1)
            elif to != "now":
                # Split failed window into smaller segments
                print(f"Splitting window {key} into smaller segments...")
                new_windows = split_window(frm, to)
                windows.pop(i)
                for w in reversed(new_windows):
                    windows.insert(i, w)
                # Update progress bar total since we added windows
                pbar.total = len(windows)
                pbar.refresh()
                # Don't increment i - process the first new window next
            else:
                # Skip live window after failures
                print(f"Skipping live window {key} after failures...")
                windows_done.add(key)
                with open(checkpoint_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "last_map": last_map,
                        "windows_done": list(windows_done)
                    }, f)
                i += 1
                pbar.update(1)

        pbar.close()

        # all windows done, remove checkpoint
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        # 4) enrich CMS metadata with last viewed dates
        enriched = enrich_metadata(cfg['cms_metadata_in'], last_map)
        save_json(enriched, cfg['output_json'])
        save_csv(enriched, cfg['output_csv'])

        print(f" Finished {name}: {cfg['output_json']}, {cfg['output_csv']}")

if __name__ == "__main__":
    sys.stdout.reconfigure(encoding='utf-8')
    main()
