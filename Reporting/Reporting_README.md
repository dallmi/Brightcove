# Reporting Runbook (Business Analyst Guide)

This document explains what the Reporting scripts do, how they work together, and how to run them end-to-end. It is written for Business Analysts who need to generate Brightcove video analytics reports without diving deep into the code.

## 1) Overall purpose

We want to produce reliable, up-to-date daily analytics for Brightcove videos across several channels (e.g., Internet, Intranet, Neo, Research). The pipeline:

- Collects video metadata (CMS) to get the universe of active, published videos for selected years.
- Quickly determines which videos had any views in the last 90 days (to limit costly API calls).
- Fetches detailed daily analytics per video and day.
- Produces consolidated CSVs suitable for downstream reporting and analysis.
- Concatenates yearly outputs into a single multi-year deliverable.

The approach balances completeness and cost by narrowing analytics calls to the videos that actually matter (recently viewed).

---

## 2) Scripts overview and how they fit together

The Reporting folder contains four scripts that should be run in sequence:

1. 1_cms_metadata_Reporting
2. 2_VideoCache.py
3. 3_daily.py
4. 4_concat.py

High-level dependencies and flow:
- 1_cms_metadata_Reporting produces per-account CMS JSON/CSV files for the selected years.
- 2_VideoCache.py reads those CMS files, queries analytics to find last viewed date per video, and outputs “filtered” per-account cache CSV/JSON files.
- 3_daily.py reads those filtered cache files and pulls detailed daily metrics per video, producing one or more consolidated CSVs per time window (e.g., 2025).
- 4_concat.py concatenates annual CSVs (e.g., 2024 + 2025) into final, multi-year outputs.

---

## 3) Prerequisites

- Python 3.x
- Install required packages:
  ```bash
  pip install requests pandas tqdm
  ```
- A `secrets.json` file in the project root with Brightcove API credentials and optional proxies:
  ```json
  {
    "client_id": "YOUR_BRIGHTCOVE_CLIENT_ID",
    "client_secret": "YOUR_BRIGHTCOVE_CLIENT_SECRET",
    "proxies": {
      "http": "http://your-proxy:port",
      "https": "http://your-proxy:port"
    }
  }
  ```
  - If you don’t use a proxy, either remove the `proxies` entry or set `use_proxies = False` in the scripts.

- Folder structure: the scripts expect to read/write relative paths like `json/`, `csv/`, `2024/`, `2025/`. Ensure these folders exist or let the scripts create them where implemented.

---

## 4) Script-by-script details

### 4.1 1_cms_metadata_Reporting

Purpose:
- Fetch CMS video metadata (IDs and other fields) for selected years, across multiple Brightcove accounts.
- Filter to videos that are ACTIVE and published to reduce downstream API load.

Key settings in the script:
- years_to_fetch = ["2024", "2025"]
- Accounts and output files:
  - Internet (account_id 1197194721001): json/internet_cms_2024_2025.json, csv/internet_cms_2024_2025.csv
  - Intranet (4413047246001): json/intranet_cms_2024_2025.json, csv/intranet_cms_2024_2025.csv
  - Neo (5972928207001): json/neo_cms_2024_2025.json, csv/neo_cms_2024_2025.csv
  - Research (3467683096001): json/research_cms_2024_2025.json, csv/research_cms_2024_2025.csv
  - Research internal (3731172721001): json/research_internal_cms_2024_2025.json, csv/research_internal_cms_2024_2025.csv

Inputs:
- secrets.json (client_id, client_secret, optional proxies)
- years_to_fetch list in the script

Outputs:
- Per-account JSON: all CMS videos fetched
- Per-account CSV: filtered ACTIVE + published videos created in the chosen years
  - Fields include: id, name, created_at, published_at, original_filename, created_by, duration, state, reference_id, tags, plus selected custom_fields (video_content_type, video_length, video_category, country, language, business_unit)

Example usage:
```bash
python Reporting/1_cms_metadata_Reporting
```

Example outcome (Internet channel):
- JSON: json/internet_cms_2024_2025.json
- CSV: csv/internet_cms_2024_2025.csv


### 4.2 2_VideoCache.py

Purpose:
- Take the per-account CMS output and query analytics to find the last date each video was viewed (within a configured rolling window, e.g., last 90 days).
- Merge last viewed date with CMS, and output per-account “cache” files used by the daily script.
- This step reduces the number of per-video/day calls later by filtering to videos with recent activity.

Key settings:
- DAYS_BACK = 90 (window to scan for last views)
- ACCOUNTS mapping points to the CMS JSON files created in step 1 and defines per-account outputs:
  - For example (Internet):
    - CMS_METADATA_PATH: json/internet_cms_2024_2025.json
    - OUTPUT_PATH: csv/internet_cms_cache_2024_2025.csv
    - OUTPUT_JSON: json/internet_cms_cache_2024_2025.json
- The script writes temporary analytics cache files like analytics_cache_internet_YYYY-MM-DD_to_YYYY-MM-DD.json for resume/efficiency.

Inputs:
- secrets.json (client credentials, optional proxies)
- CMS JSON files from 1_cms_metadata_Reporting (per account)
- DAYS_BACK setting

Outputs:
- Per-account merged CSV and JSON files containing only videos with recent views:
  - Example: csv/internet_cms_cache_2024_2025.csv
  - Example: json/internet_cms_cache_2024_2025.json

Example usage:
```bash
python Reporting/2_VideoCache.py
```

Example result:
- “Processed account: internet” followed by counts and saved rows
- Writes outputs for each account configured in ACCOUNTS


### 4.3 3_daily.py

Purpose:
- Read the per-account “cache” JSONs from step 2 (videos with recent views) and fetch detailed daily analytics per video.
- Produces consolidated CSVs for the specified window (e.g., 2025) and maintains a checkpoint log for resuming if interrupted.

Key settings:
- from_date = "2025-01-01"
- to_date = current date (dynamic)
- Accounts and input paths:
  - Internet: json/internet_cms_cache_2024_2025.json
  - Intranet: json/intranet_cms_cache_2024_2025.json
  - Neo: json/neo_cms_cache_2024_2025.json
  - Research: json/research_cms_cache_2024_2025.json
  - Research internal: json/research_internal_cms_cache_2024_2025.json
- Outputs:
  - master_csv (Internet + Intranet): 2025/daily_analytics_summary_2025_01_01_to_YYYY_MM_DD.csv
  - master_csv_research (Neo + Research + Research_internal): 2025/daily_analytics_summary_research_2025_01_01_to_YYYY_MM_DD.csv
  - checkpoint_file: checkpoint_log.txt (append-only JSON lines used to resume by last date per video/account)

What it fetches (examples of metrics):
- Per video/day:
  - video_view, video_impression, play_rate, engagement_score
  - video_engagement_1/25/50/75/100
  - video_percent_viewed, video_seconds_viewed
- Device breakdown per day (views_desktop, views_mobile, views_tablet, views_other)
- Carries forward CMS fields (name, created_at, published_at, original_filename, created_by, tags, reference_id, custom fields, duration)

CSV fields written include:
- channel, account_id, video_id, name, date
- video_view, views_desktop, views_mobile, views_tablet, views_other
- video_impression, play_rate, engagement_score
- video_engagement_1, _25, _50, _75, _100
- video_percent_viewed, video_seconds_viewed
- created_at, published_at, original_filename, created_by
- video_content_type, video_length, video_duration, video_category, country, language, business_unit, tags, reference_id
- report_generated_on

Inputs:
- secrets.json
- Per-account cache JSONs from step 2
- Date range (from_date/to_date)

Outputs:
- 2025/daily_analytics_summary_...csv (Internet/Intranet)
- 2025/daily_analytics_summary_research_...csv (Neo/Research/Research_internal)
- checkpoint_log.txt (for resuming incrementally)

Example usage:
```bash
python Reporting/3_daily.py
```

Example behavior:
- Resumes from checkpoint by remembering the last processed date per video
- Writes progress incrementally to checkpoint_log.txt and updates ETA


### 4.4 4_concat.py

Purpose:
- Concatenate annual CSV outputs into combined multi-year datasets for each category (e.g., “internet” and “research”).
- Writes final files to a shared path.

Key settings:
- Input files dictionary (by category and year). Example:
  - internet:
    - 2024: 2024/daily_analytics_summary_2023_01_01_to_2024_12_31.csv
    - 2025: 2025/daily_analytics_summary_2025_01_01_to_YYYY_MM_DD.csv
  - research:
    - 2024: 2024/daily_analytics_summary_research_2023_01_01_to_2024_12_31.csv
    - 2025: 2025/daily_analytics_summary_research_2025_01_01_to_YYYY_MM_DD.csv
- Output path:
  - Q:/Brightcove/Reporting/daily_analytics_2023_2024_2025_{category}.csv
  - If you do not have a Q: drive, update the script to a local path.

Inputs:
- The annual CSVs produced in previous steps, placed in 2024/ and 2025/ folders.

Outputs:
- Combined multi-year CSV per category:
  - daily_analytics_2023_2024_2025_internet.csv
  - daily_analytics_2023_2024_2025_research.csv

Example usage:
```bash
python Reporting/4_concat.py
```

---

## 5) End-to-end sequence (Runbook)

1) Prepare environment
- Create/verify secrets.json with valid Brightcove credentials.
- Ensure folders are in place: json/, csv/, 2024/, 2025/.

2) Fetch CMS metadata (selected years)
```bash
python Reporting/1_cms_metadata_Reporting
```
- Outputs per-account CMS JSON and CSV for the configured years.

3) Build last-viewed cache (last 90 days)
```bash
python Reporting/2_VideoCache.py
```
- Reads CMS JSONs from step 2.
- Writes per-account filtered cache CSVs/JSONs to csv/ and json/.

4) Pull daily analytics for the chosen window (e.g., 2025-01-01 to today)
```bash
python Reporting/3_daily.py
```
- Uses per-account filtered cache JSONs from step 3.
- Produces consolidated CSVs under 2025/.
- Safe to stop and resume thanks to checkpoint_log.txt.

5) Concatenate yearly outputs
```bash
python Reporting/4_concat.py
```
- Combines 2024 + 2025 files into final multi-year CSVs under Q:/Brightcove/Reporting (or your chosen path).

---

## 6) Configuration tips and examples

- Changing years for CMS fetch:
  - In 1_cms_metadata_Reporting, update:
    ```python
    years_to_fetch = ["2024", "2025"]
    ```
  - Ensure the downstream scripts’ input file names match the new year suffix.

- Changing the rolling window for “recently viewed”:
  - In 2_VideoCache.py:
    ```python
    DAYS_BACK = 90
    ```

- Changing daily analytics date range:
  - In 3_daily.py:
    ```python
    from_date = "2025-01-01"
    to_date = datetime.now().strftime("%Y-%m-%d")
    ```

- No proxy environment:
  - In any script that sets `use_proxies = True`, change to:
    ```python
    use_proxies = False
    proxies = None
    ```

- Output destination for final concatenated files:
  - In 4_concat.py, change:
    ```python
    combined_df.to_csv("Q:/Brightcove/Reporting/...", index=False)
    ```
    to a path available on your machine or server.

---

## 7) Expected runtimes (order-of-magnitude)

- 1_cms_metadata_Reporting: minutes for multiple years (bulk CMS fetch without daily granularity).
- 2_VideoCache.py: minutes (paged analytics, cached).
- 3_daily.py: hours depending on number of videos and dates (optimized with checkpointing and recent-view filter).
- 4_concat.py: seconds to a few minutes.

---

## 8) Troubleshooting

- Authentication errors (401/403):
  - Check client_id and client_secret in secrets.json.
  - Ensure token refresh is working; rerun the script.

- Network/proxy issues:
  - If using a corporate proxy, confirm the proxy settings in secrets.json.
  - If not required, set `use_proxies = False`.

- Missing files:
  - Make sure step outputs exist before running the next script (e.g., CMS JSONs exist before running 2_VideoCache.py).
  - Ensure the 2024/ and 2025/ directories contain the expected files before concatenation.

- Slow runs / interruptions:
  - 3_daily.py is resumable via checkpoint_log.txt. Re-run it to continue from where it left off.

---

## 9) Data glossary (selected fields)

- video_view: Number of views for a video on a given date.
- video_impression: Number of times a video impression occurred.
- play_rate: Plays divided by impressions (as a rate).
- engagement_score: Brightcove’s engagement metric.
- video_engagement_1/25/50/75/100: Audience retention at the specified percentiles.
- video_percent_viewed: Average percent of video watched.
- video_seconds_viewed: Total seconds watched.
- views_desktop/mobile/tablet/other: Device-type breakdown of views.
- Custom fields: video_content_type, video_length, video_category, country, language, business_unit (if present in CMS metadata).

---

## 10) Summary

- Run scripts in order: 1 → 2 → 3 → 4.
- Ensure secrets.json and folder paths are correct.
- Use the checkpoint to safely resume long analytics runs.
- Final outputs are consolidated CSVs ready for reporting.

If you need this guide adapted for a different year window, different channels, or a different output location, update the corresponding configuration sections in each script as described above.