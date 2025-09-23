# Harper Project Documentation (Business Analyst Friendly)

## 1) Purpose and Outcomes

This project automates the end-to-end retrieval and delivery of Brightcove video data for several corporate Brightcove accounts. It has three main objectives:
- Discover and catalog all videos per Brightcove account (metadata).
- Retrieve viewership/engagement metrics (typically broken down by day and by key attributes) for those videos.
- Package and copy the generated data files to downstream locations for reporting, analytics, or archival.

Expected outcomes:
- Up-to-date JSON and CSV files describing each video and its custom fields (per account).
- CSV files with time-series “last viewed” analytics suitable for dashboards and further analysis.
- A repeatable sequence that can be scheduled (e.g., daily) and monitored.


## 2) High-level Flow

1. 1_cms_metadata.py
   - Authenticates to Brightcove.
   - Iterates through configured accounts.
   - Retrieves the full list of videos and their metadata (including custom fields).
   - Writes results to JSON and CSV, per account.

2. 2_LastViewed.py
   - Authenticates to Brightcove.
   - Retrieves analytics (e.g., views by day, filtered/aggregated by relevant dimensions and custom fields) for the configured accounts and/or video IDs.
   - Writes results to CSV, per account and time window.

3. 3_CopyFiles
   - Copies the CSV/JSON outputs to target destinations (e.g., a shared drive, cloud bucket, or a delivery folder for BI tools).

Data dependencies:
- 2_LastViewed.py may optionally use the video IDs discovered by 1_cms_metadata.py (the JSON output) to focus analytics retrieval on known videos.
- 3_CopyFiles expects the CSV/JSON files created by the first two steps.

Recommended run order:
- Step 1: 1_cms_metadata.py
- Step 2: 2_LastViewed.py
- Step 3: 3_CopyFiles


## 3) Prerequisites

- Python 3.9+ (recommended)
- Network access to Brightcove APIs
- Brightcove OAuth credentials with appropriate scopes:
  - Client ID
  - Client Secret
- Optional: Corporate proxy configuration (HTTP/HTTPS) if required by your network
- Folders present in the repo working directory:
  - json/
  - csv/
  (Create these if they do not exist.)

Python packages (install with `pip install -r requirements.txt` or individually):
- requests
- tqdm

Note: Standard library modules used include json, csv, base64, time, and datetime.


## 4) Configuration

The scripts expect a file named `secrets.json` in the working directory with credentials and (optionally) proxies. Example:

```json
{
  "client_id": "YOUR_BRIGHTCOVE_CLIENT_ID",
  "client_secret": "YOUR_BRIGHTCOVE_CLIENT_SECRET",
  "proxies": {
    "http": "http://your.proxy:8080",
    "https": "http://your.proxy:8080"
  }
}
```

- `proxies` is optional. If your environment does not require a proxy, it can be omitted. The code enables proxy use by default; if you don’t use a proxy, set the flag in the script accordingly.


## 5) Scripts, Inputs, Outputs, and Examples

### A) 1_cms_metadata.py — Fetch video catalog (CMS metadata)

What it does (summary):
- Authenticates to Brightcove and fetches all videos for each configured account.
- Captures key metadata fields and custom fields.
- Writes:
  - a JSON file with raw API payload of all videos
  - a CSV file with a curated set of columns, per account

Key inputs:
- `secrets.json` for OAuth credentials (and optional proxies).
- Internal “accounts” mapping (in the script) containing Brightcove account IDs and output filenames. Example accounts in the script:
  - Internet: 1197194721001
  - Intranet: 4413047246001
  - neo: 5972928207001
  - research: 3467683096001
  - research_internal: 3731172721001
  - impact: 968049871001
  - circleone: 6283605170001
  - digital_networks_events: 4631489639001
  - fa_web: 807049819001
  - SuMiTrust: 5653786046001
  - MyWay: 6300219615001

Processing highlights:
- Retrieves all videos via the Brightcove CMS API using pagination (limit/offset).
- Persists the full results to JSON.
- Creates a CSV with selected standard fields plus custom fields captured in Brightcove.

Outputs (per account):
- JSON: json/<channel>_cms_metadata.json
- CSV: csv/<channel>_cms_metadata.csv

CSV columns (from the script):
- Standard fields:
  - account_id, id, name, original_filename, description, updated_at, created_at, published_at, created_by, ad_keys, clip_source_video_id, complete, cue_points, delivery_type, digital_master_id, duration, economics, folder_id, geo, has_digital_master, images, link, long_description, projection, reference_id, schedule, sharing, state, tags, text_tracks, transcripts, updated_by, playback_rights_id, ingestion_profile_id
- Custom fields (as configured in the script at the time of writing):
  - video_content_type, relatedlinkname, relatedlink, country, language, business_unit, video_category, video_length, video_owner_email, 1a_comms_sign_off, 1b_comms_sign_off_approver, 2a_data_classification_disclaimer, 3a_records_management_disclaimer, 4a_archiving_disclaimer_comms_branding, 4b_unique_sharepoint_id

Run time:
- Approx. 8 minutes for all accounts (varies by total videos and network).

Simple example (how to run):
- From the repo root (with json/ and csv/ folders created):
  - `python Harper/1_cms_metadata.py`

Example output snippet (CSV):
```
account_id,id,name,original_filename,...,video_content_type,country,language,...
1197194721001,1234567890,"Quarterly Results","Q1_Results.mp4",...,"internal","US","en",...
```

Notes:
- “tags” in the output are joined as a comma-separated string.
- If your environment doesn’t require a proxy, disable proxy usage in the script before running, or ensure `proxies` is not used.


### B) 2_LastViewed.py — Retrieve “Last Viewed” analytics (time series)

What it does (summary):
- Authenticates to Brightcove and calls analytics endpoints.
- Retrieves metrics broken down by day and relevant dimensions/custom fields.
- Produces CSV files per account with daily view metrics.

Key inputs:
- `secrets.json` (same as above).
- Likely uses the same accounts list or a similar mapping to decide which accounts to process.
- Date range (either configured within the script or via parameters) to scope analytics calls.
- Optionally, the video IDs from the JSON produced by 1_cms_metadata.py to restrict analytics to known videos.

Processing highlights (typical approach for Brightcove Analytics):
- Loops over date ranges (daily buckets).
- Requests daily metrics (e.g., video views) and may include breakdowns (e.g., by custom fields).
- Aggregates and writes results to CSV.

Outputs (per account):
- CSV: e.g., csv/<channel>_last_viewed_<date_range>.csv

Typical CSV columns (may vary based on the exact implementation):
- date
- account_id
- video_id
- video_name (when resolvable)
- views (or video_view)
- plays_requested
- engagement_score (if requested)
- custom fields mirrored from CMS for attribution (e.g., business_unit, video_category, video_content_type)
- any additional dimensions (device, country, etc., if included by the script)

Simple example (how to run):
- `python Harper/2_LastViewed.py`
- If the script supports parameters: `python Harper/2_LastViewed.py --start 2025-01-01 --end 2025-01-31`

Example output snippet (CSV):
```
date,account_id,video_id,views,video_content_type,business_unit
2025-01-01,1197194721001,1234567890,42,internal,Research
2025-01-02,1197194721001,1234567890,37,internal,Research
```

Notes:
- The “LastViewed” script is referenced within 1_cms_metadata.py comments as doing “break down by days and custom columns.”
- Exact column set and parameters depend on the implementation in 2_LastViewed.py.


### C) 3_CopyFiles — Distribute generated files

What it does (summary):
- Copies or moves the generated CSV/JSON files to target locations for downstream consumption (e.g., a shared folder, BI ingestion directory, or cloud storage).

Key inputs:
- Source directories: json/, csv/
- Destination path(s): configured within this script (e.g., a network path or local outbox).
- Inclusion/exclusion patterns (e.g., only the latest run, or files matching a date).

Processing highlights:
- Enumerates the files created by 1_cms_metadata.py and 2_LastViewed.py.
- Copies with overwrite/safeguards as implemented.
- May timestamp the destination folders or files.

Outputs:
- Files in destination folder(s) for stakeholders and systems.

Simple example (how to run):
- `python Harper/3_CopyFiles` (if it is a Python script with no extension) or `python Harper/3_CopyFiles.py` (if renamed with .py), or `./Harper/3_CopyFiles` if it’s an executable script.
- After successful run, the destination will contain the CSV/JSON created earlier.

Notes:
- Verify destination paths and access permissions before running.
- If the script is a shell script, run it with appropriate execute permissions or via bash.


## 6) Runbook (Sequence, Frequency, and Monitoring)

Recommended sequence:
1) Fetch metadata
   - Command: `python Harper/1_cms_metadata.py`
   - Verify: New files appear in json/ and csv/ per account. Check console logs for completion messages.

2) Fetch analytics
   - Command: `python Harper/2_LastViewed.py` (optionally with date range parameters)
   - Verify: New CSV files in csv/ with daily metrics. Spot-check row counts vs. expected volume.

3) Distribute files
   - Command: `python Harper/3_CopyFiles` (or the appropriate invocation)
   - Verify: Files appear at the intended destination. Confirm consumer can read them.

Typical frequency:
- Daily for analytics (overnight run).
- Weekly or monthly refresh for full metadata (unless frequent updates are required).

Monitoring:
- Review script console output and progress bars (tqdm).
- Check for non-zero exit codes and missing outputs.
- Optionally, wire into a scheduler (e.g., cron, Task Scheduler, or CI) with log capture.


## 7) Troubleshooting and Tips

- Authentication failures:
  - Ensure `secrets.json` has valid `client_id` and `client_secret`.
  - If using a proxy, confirm the proxy settings and network access to Brightcove endpoints.

- Empty or partial outputs:
  - Confirm the account IDs are correct in the script’s accounts mapping.
  - Ensure the output folders `json/` and `csv/` exist and are writable.
  - For analytics, verify the date range includes days with activity.

- Performance:
  - Metadata fetch is batched; runtime depends on number of videos and network latency.
  - Analytics fetches that break down by day can take longer; consider limiting the date range for testing.

- Fields and custom fields:
  - Custom fields must exist in Brightcove video settings to appear in outputs.
  - Tags are exported as comma-separated strings.


## 8) How to Extend

- Add a new account:
  - Edit the accounts mapping in 1_cms_metadata.py (and, if needed, in 2_LastViewed.py) with a new `account_id` and desired output filenames.

- Add new fields to CSV:
  - Update the `csv_fields` (and any custom fields list) in 1_cms_metadata.py to include additional properties returned by the CMS API.

- Change destinations:
  - Adjust paths and logic in 3_CopyFiles for new target folders or transfer mechanisms.


## 9) References

- Brightcove CMS API: Video endpoints (list, search)
- Brightcove Analytics API: Time-series and breakdowns by dimensions and fields
- Organization proxy/network documentation


## 10) Glossary

- Brightcove: A video hosting and analytics platform.
- CMS (Content Management System) Metadata: Descriptive information about videos (e.g., title, created_at, tags, and custom business fields).
- Analytics “Last Viewed”: Daily time series of view events per video/account, used to understand engagement over time.
- Custom fields: Organization-specific fields configured in Brightcove to categorize or add governance attributes to videos.