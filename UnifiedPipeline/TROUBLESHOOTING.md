# Troubleshooting Guide

## Issue 1: Historical Data Being Reprocessed After CMS Update

### Symptom
After running `1_cms_metadata.py` to pull recent CMS data, the pipeline processes 2024 and 2025 data again, even though a historical run completed a week ago.

### Root Cause
This is **expected behavior** when:
1. **New videos were added** - Script 1 fetches the latest video list from CMS. Any new videos that weren't in the previous run need their full historical data (2024-2025) fetched.
2. **Videos were re-created** - If videos were deleted and re-uploaded with new IDs, they'll be treated as new.

### How It Works
The pipeline uses `video_max_dates` (a dictionary mapping `(account_id, video_id)` → `last_date_processed`) to track which videos already have data:

```python
# Line 662 in 3_daily_analytics.py
video_max_dates = get_all_video_max_dates(conn)

# Line 414-415: Check if video exists in DB
last_processed = video_max_dates.get(key)
if not last_processed:
    # This is a NEW video - fetch ALL historical data
```

**Key insight**: Videos NOT in `video_max_dates` require full historical processing!

### Diagnosis

Run the diagnostic script to see exactly which videos are missing:

```bash
cd UnifiedPipeline/scripts
python diagnostic_video_max_dates.py
# Or for specific account:
python diagnostic_video_max_dates.py --account Internet
```

This will show:
- How many videos are in CMS vs DuckDB
- Which specific videos are missing (with creation dates)
- Why the pipeline is fetching historical data

**Example output:**
```
Internet (Account ID: 123456789):
  Videos in CMS:           1,250
  Videos in DuckDB:        1,200
  Missing from DuckDB:     50 ⚠️

  🔍 Sample of missing video IDs (first 10):
     6234567890001 - Created: 2024-11-15 - New Product Launch Video
     6234567890002 - Created: 2025-01-03 - Q1 Town Hall
     ...
```

### What's Normal vs Concerning

**✓ Normal:**
- 5-50 new videos after monthly CMS update → a few minutes of historical processing
- Videos created recently (last 30 days) need their full history fetched

**⚠️ Concerning:**
- 1000+ videos "missing" from DuckDB → possible data type mismatch or corrupted checkpoint
- Videos created years ago but not in DuckDB → pipeline may have failed previously

### Prevention

To minimize reprocessing:
1. **Run scripts in order**: Always run 1→2→3 together
2. **Checkpoint regularly**: See Issue 2 below
3. **Monitor for failures**: Check logs for API errors that cause incomplete runs

---

## Issue 2: DuckDB WAL File Growing, Main DB Not Updated

### Symptom
During long pipeline runs:
- `.wal` file grows to hundreds of MB
- Main `.duckdb` file stays small/unchanged
- Unclear if data is being saved

### Root Cause
This is **normal DuckDB behavior** with Write-Ahead Logging (WAL):

1. **WAL mode** (default): All writes go to `.wal` file first
2. **Checkpoint**: Merges WAL into main `.duckdb` file
3. **When checkpoints happen**:
   - When connection is closed (line 690 in script)
   - Manually with `conn.execute("CHECKPOINT")`
   - Rarely: DuckDB auto-checkpoint (every ~1GB WAL)

**Problem**: For multi-hour runs, changes stay in WAL until script completes!

### Why This Matters

**Risks:**
- If process crashes → WAL changes may be lost (though DuckDB usually recovers)
- Disk space: WAL can grow large (100s of MB)
- Unclear progress: Can't see main DB file growing

**Benefits of frequent checkpoints:**
- Changes persisted immediately
- Can monitor progress by checking DB file size
- Faster recovery if process crashes

### Solution

I've added **automatic checkpoints** to `3_daily_analytics.py`:

1. **After every batch** (every ~3000 rows):
   ```python
   conn.execute("CHECKPOINT")
   ```

2. **After each account completes**:
   ```python
   logger.info(f"Checkpointing {account_name} {year} to disk...")
   conn.execute("CHECKPOINT")
   ```

This ensures:
- Changes written to main DB every few minutes
- WAL file stays small (< 10 MB typically)
- Progress visible in main DB file size

### Manual Checkpoint

If your process is currently running with a large WAL file, you can checkpoint manually:

```bash
cd UnifiedPipeline/scripts

# Checkpoint main analytics DB
python checkpoint_duckdb.py

# Checkpoint account-specific DB
python checkpoint_duckdb.py --account Internet

# Just check stats without checkpointing
python checkpoint_duckdb.py --stats
```

**⚠️ Important**: Only checkpoint if:
- Process has completed
- OR process has crashed/been terminated
- DO NOT checkpoint while process is actively writing!

### Monitoring

Check WAL file status:
```bash
# From UnifiedPipeline/output directory
ls -lh *.duckdb*

# Example output:
# -rw-r--r--  1 user staff  2.1G analytics.duckdb
# -rw-r--r--  1 user staff  245M analytics.duckdb.wal  ← Should be small!
```

**Healthy:**
- Main DB: 1-5 GB
- WAL file: < 50 MB or doesn't exist

**Needs checkpoint:**
- WAL file: > 200 MB
- Main DB not growing despite hours of runtime

---

## Quick Reference

### Before Running Pipeline

```bash
# 1. Check current database state
python checkpoint_duckdb.py --stats

# 2. Run diagnostic to understand what will be processed
python diagnostic_video_max_dates.py
```

### During Pipeline Run

**Normal logging:**
```
Processing: Internet 2024
  Videos in CMS:           1,250
  Videos in DuckDB:        1,200
  Videos needing API calls: 50      ← Expected if you have new videos
  Estimated time: ~2.1 minutes

Checkpointing Internet 2024 to disk...  ← Checkpoint happening
```

**Monitor WAL file:**
```bash
watch -n 30 'ls -lh output/*.duckdb*'
# Should see WAL file stay small after checkpoints
```

### After Pipeline Completes

```bash
# Verify no WAL file remains
ls output/*.wal
# Should be: "No such file"

# Check final stats
python checkpoint_duckdb.py --stats
```

---

## Summary

**Issue 1 (Historical reprocessing):**
- ✓ Usually normal - new videos need historical data
- ✓ Use diagnostic script to verify
- ⚠️ Only concerning if 100s+ of old videos are missing

**Issue 2 (WAL file):**
- ✓ Now fixed with automatic checkpoints
- ✓ Manual checkpoint utility available
- ⚠️ Don't checkpoint while process is writing!

Both issues are now diagnosed and solved! 🎉
