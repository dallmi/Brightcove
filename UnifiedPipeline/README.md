# UnifiedPipeline - Brightcove Analytics Pipeline

Robust pipeline for capturing Brightcove video analytics with all metadata.

## Prerequisites

- Python 3.9+
- `pip install requests tqdm pandas openpyxl`
- `secrets.json` in the main Brightcove directory with:
  ```json
  {
    "client_id": "...",
    "client_secret": "...",
    "proxies": {"http": "...", "https": "..."}
  }
  ```

## Test Mode (recommended before first run!)

Tests the pipeline with 2 accounts (MyWay + research_internal) and only 2026 (no history):

```cmd
cd C:\path\to\Brightcove

REM Enable test mode
set PIPELINE_TEST=1

REM Run all scripts (~5-10 min instead of 5-9 hours)
python UnifiedPipeline/scripts/1_cms_metadata.py
python UnifiedPipeline/scripts/2_dt_last_viewed.py
python UnifiedPipeline/scripts/3_daily_analytics.py
python UnifiedPipeline/scripts/4_combine_output.py

REM Disable test mode
set PIPELINE_TEST=
```

**Test Config (`config/*_TEST.json`):**
- `accounts_TEST.json`: MyWay + research_internal (for gwm + research categories)
- `settings_TEST.json`: Only 2026, no historical years

**After successful test:** Delete checkpoints before production run:
```cmd
rmdir /s /q UnifiedPipeline\checkpoints
rmdir /s /q UnifiedPipeline\output
mkdir UnifiedPipeline\checkpoints
mkdir UnifiedPipeline\output
```

## Workflow

### First Run (~5-9 hours)

```cmd
cd C:\path\to\Brightcove

python UnifiedPipeline/scripts/1_cms_metadata.py      REM ~10 min
python UnifiedPipeline/scripts/2_dt_last_viewed.py    REM ~60-90 min
python UnifiedPipeline/scripts/3_daily_analytics.py   REM ~4-8 h (2024+2025+2026)
python UnifiedPipeline/scripts/4_combine_output.py    REM ~3 min
```

### Subsequent Runs (~1 hour)

```cmd
cd C:\path\to\Brightcove

python UnifiedPipeline/scripts/1_cms_metadata.py      REM ~10 min  (IMPORTANT: new videos!)
python UnifiedPipeline/scripts/2_dt_last_viewed.py    REM ~5-10 min (incremental!)
python UnifiedPipeline/scripts/3_daily_analytics.py   REM ~30-60 min (only 2026)
python UnifiedPipeline/scripts/4_combine_output.py    REM ~3 min
```

## Why Run Each Step Every Time?

| Script | Why run every time? |
|--------|---------------------|
| `1_cms_metadata` | Capture new videos since last run |
| `2_dt_last_viewed` | Update dt_last_viewed (incremental since last run) |
| `3_daily_analytics` | Historical data is automatically skipped |
| `4_combine_output` | Regenerate CSVs |

### Incremental Mode (Script 2)

From the second run onwards, `2_dt_last_viewed.py` works incrementally:
- Only loads data since the last run (+ 3 days overlap for analytics latency)
- Updates `dt_last_viewed` only when new date > existing date
- **~98% fewer API calls** for monthly runs

Configurable in `settings.json`:
```json
"windows": {
  "incremental_overlap_days": 3
}
```

To force a full refresh: delete `checkpoints/analytics_checkpoint.json`.

## Data Strategy

```
┌─────────────────────────────────────────────────────┐
│  HISTORICAL (2024 + 2025)                           │
│  • All videos (no filter)                           │
│  • One-time during first run                        │
│  • Checkpoint: daily_historical.jsonl               │
└─────────────────────────────────────────────────────┘
                      +
┌─────────────────────────────────────────────────────┐
│  CURRENT (2026)                                     │
│  • Only videos with views in last 90 days           │
│  • Incremental on each run                          │
│  • Checkpoint: daily_current.jsonl                  │
└─────────────────────────────────────────────────────┘
                      =
┌─────────────────────────────────────────────────────┐
│  OUTPUT                                             │
│  • daily_analytics_2024_*.csv                       │
│  • daily_analytics_2025_*.csv                       │
│  • daily_analytics_2026_*.csv                       │
│  • daily_analytics_2024_2025_2026_all.csv           │
└─────────────────────────────────────────────────────┘
```

## Folder Structure

```
UnifiedPipeline/
├── config/
│   ├── accounts.json       # 11 accounts + categories
│   └── settings.json       # Years, retry, etc.
├── checkpoints/
│   ├── analytics_checkpoint.json # dt_last_viewed status + last_run_date
│   ├── daily_historical.jsonl    # 2024+2025 data
│   ├── daily_current.jsonl       # 2026 data
│   └── historical_status.json    # Tracking which years are complete
├── output/
│   ├── cms/                # CMS metadata (JSON/CSV)
│   ├── analytics/          # dt_last_viewed + enriched JSON
│   ├── daily/              # Final analytics CSVs
│   └── life_cycle_mgmt/    # Excel files for lifecycle management
│       ├── Internet_cms.xlsx
│       ├── Intranet_cms.xlsx
│       ├── neo_cms.xlsx
│       └── ... (all 11 accounts)
├── logs/                   # Log files
└── scripts/
    ├── shared.py           # Shared utilities
    ├── 1_cms_metadata.py
    ├── 2_dt_last_viewed.py
    ├── 3_daily_analytics.py
    └── 4_combine_output.py
```

## Lifecycle Management Output (Excel)

After each run of `2_dt_last_viewed.py`, Excel files are automatically generated:

```
output/life_cycle_mgmt/
├── Internet_cms.xlsx
├── Intranet_cms.xlsx
├── neo_cms.xlsx
├── research_cms.xlsx
├── research_internal_cms.xlsx
├── impact_cms.xlsx
├── circleone_cms.xlsx
├── digital_networks_events_cms.xlsx
├── fa_web_cms.xlsx
├── SuMiTrust_cms.xlsx
└── MyWay_cms.xlsx
```

These files match the Harper format (`channel_cms.xlsx`) and contain:
- All CMS metadata
- `dt_last_viewed` (last viewed date)
- All `cf_*` custom fields

## Accounts (11)

| Account | Category |
|---------|----------|
| Internet, Intranet | internet_intranet |
| neo, research, research_internal | research |
| impact, circleone, fa_web, SuMiTrust, MyWay | gwm |
| digital_networks_events | events |

## Output Columns (44)

Reporting fields (32) + Harper fields (12):
- `dt_last_viewed` - Last viewed date
- `cf_*` - All custom fields (Owner, Compliance, etc.)

## Error Handling

- **5 retries** with exponential backoff + jitter
- **Window splitting** on API errors (down to day level)
- **Checkpointing** after each video/window
- On interruption, simply restart - it will automatically resume

## Configuration Customization

`config/settings.json`:
```json
{
  "daily_analytics": {
    "historical_years": [2024, 2025],
    "current_year": 2026,
    "days_back_filter": 90
  }
}
```

For 2027: Change `current_year` to 2027, add 2026 to `historical_years`.
