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
        "cms_metadata_in": 'json/intranet_cms_metadata.json',
        "csv_output_file": f'csv/intranet_cms.csv',
        "json_output_file": f'json/intranet_cms.json',
    },
    "neo": {
        "account_id": "5972928207001",
        "cms_metadata_in": 'json/neo_cms_metadata.json',
        "csv_output_file": f'csv/neo_cms.csv',
        "json_output_file": f'json/neo_cms.json',
    },
    "research": {
        "account_id": "3467683096001",
        "cms_metadata_in": 'json/research_cms_metadata.json',
        "csv_output_file": f'csv/research_cms.csv',
        "json_output_file": f'json/research_cms.json',
    },
    "research_internal": {
        "account_id": "3731172721001",
        "cms_metadata_in": 'json/research_internal_cms_metadata.json',
        "csv_output_file": f'csv/research_internal_cms.csv',
        "json_output_file": f'json/research_internal_cms.json',
    },
    "impact": {
        "account_id": "968049871001",
        "cms_metadata_in": 'json/impact_cms_metadata.json',
        "csv_output_file": f'csv/impact_cms.csv',
        "json_output_file": f'json/impact_cms.json',
    },
    "circleone": {
        "account_id": "6283605170001",
        "cms_metadata_in": 'json/circleone_cms_metadata.json',
        "csv_output_file": f'csv/circleone_cms.csv',
        "json_output_file": f'json/circleone_cms.json',
    },
    "digital_networks_events": {
        "account_id": "4631489639001",
        "cms_metadata_in": 'json/digital_networks_events_cms_metadata.json',
        "csv_output_file": f'csv/digital_networks_events_cms.csv',
        "json_output_file": f'json/digital_networks_events_cms.json',
    },
    "fa_web": {
        "account_id": "807049819001",
        "cms_metadata_in": 'json/fa_web_cms_metadata.json',
        "csv_output_file": f'csv/fa_web_cms.csv',
        "json_output_file": f'json/fa_web_cms.json',
    },
    "SuMiTrust": {
        "account_id": "5653786046001",
        "cms_metadata_in": 'json/Sumi_Trust_cms_metadata.json',
        "csv_output_file": f'csv/Sumi_Trust_cms.csv',
        "json_output_file": f'json/Sumi_Trust_cms.json',
    },
    #"SeniorMgmtInternal": {
    #    "account_id": "4571860231001",
    #    "csv_output_file": f'csv/SeniorMgmtInternal_cms.csv',
    #    "json_output_file": f'json/SeniorMgmtInternal_cms.json',
    #},
    "MyWay": {
        "account_id": "6300219615001",
        "cms_metadata_in": 'json/MyWay_cms_metadata.json',
        "csv_output_file": f'csv/MyWay_cms.csv',
        "json_output_file": f'json/MyWay_cms.json',
    },
    "Internet": {
        "account_id": "1197194721001",
        "cms_metadata_in": 'json/internet_cms_metadata.json',
        "csv_output_file": f'csv/internet_cms.csv',
        "json_output_file": f'json/internet_cms.json',
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


# def make_year_window(start_iso, end_iso):
#     """
#     Split the range from start_iso to end_iso into calendar-year windows.
#     The final window will use "now" for live data.
#     """
#     start_year = datetime.strptime(start_iso, "%Y-%m-%d").year
#     end_year = datetime.strptime(end_iso, "%Y-%m-%d").year
#     windows = []
#     for y in range(start_year, end_year + 1):
#         frm = f"{y}-01-01"
#         if y < end_year:
#             to = f"{y}-12-31"
#         else:
#             to = "now"
#         windows.append((frm, to))
#     return windows

def make_year_windows(start_iso, end_iso):
    """
    Split the range from start_iso to end_iso into half-year windows (Jan-Jun, Jul-Dec).
    The final window will use "now" if end_iso is the current year.
    """
    start_dt = datetime.strptime(start_iso, "%Y-%m-%d")
    end_dt = datetime.strptime(end_iso, "%Y-%m-%d")
    windows = []
    year = start_dt.year:
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
        # if first_start <= end_dt:
        #     frm = max(first_start, start_dt).strftime("%Y-%m-%d")
        #     to_dt = min(first_end, end_dt)
        #     to = to_dt.strftime("%Y-%m-%d") if to_dt < end_dt or year < end_dt.year else "now"
        #     windows.append((frm, to))
        if frm_dt <= to_dt:
            frm = frm_dt.strftime("%Y-%m-%d")
            to = to_dt.strftime("%Y-%m-%d") if (to_dt < end_dt or year < end_dt.year) else "now"
            windows.append((frm, to))
        
        #second half: only if overlapping with start_dt, end_dt
        # if second_start <= end_dt and (start_dt <= second_end):
        #     frm = max(second_start, start_dt).strftime("%Y-%m-%d")
        #     to_dt = min(second_end, end_dt)
        #     to = to_dt.strftime("%Y-%m-%d") if to_dt < end_dt or year < end_dt.year else "now"
        #     windows.append((frm, to))
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


