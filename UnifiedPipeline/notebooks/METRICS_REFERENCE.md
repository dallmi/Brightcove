# Video Analytics -- Metrics Reference

Complete guide to every metric used in the Executive Video Insights notebook.
For each metric: source, definition, example, business value, and action triggers.

---

## 1. Volume Metrics

### Views (`video_view`)

| | |
|---|---|
| **Source** | Brightcove Analytics API |
| **Type** | Integer (daily count per video) |
| **Definition** | Number of times a video started playing on a given day. Counted when playback begins, not on page load. |
| **Example** | A training video has `video_view = 347` on 2025-03-15 -- it was played 347 times that day. |
| **Business question** | How much is our video content actually being consumed? |
| **Action trigger** | Low views on important content -> investigate distribution (is it linked? promoted?). Sudden spikes -> tie to campaigns or events. |

### Impressions (`video_impression`)

| | |
|---|---|
| **Source** | Brightcove Analytics API |
| **Type** | Integer (daily count per video) |
| **Definition** | Number of times the video player was loaded on a page, regardless of whether the viewer clicked play. One page load = one impression. |
| **Example** | A video on the intranet homepage gets 5,200 impressions but only 1,800 views. Most visitors saw the player but didn't click. |
| **Business question** | How visible is our content? Are people seeing it? |
| **Action trigger** | High impressions but low views -> thumbnail or title isn't compelling enough (see Play Rate). |

### Watch Hours (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from `video_seconds_viewed` (Brightcove Analytics API) |
| **Formula** | `SUM(video_seconds_viewed) / 3600` |
| **Definition** | Total hours of video consumed across all viewers. If 100 people each watch 36 seconds, that's 1 watch hour. |
| **Example** | A channel has 12,500 watch hours in 2025 -- equivalent to someone watching video non-stop for 1.4 years. |
| **Business question** | What is the total time investment employees are making in video content? |
| **Action trigger** | Declining watch hours with stable views -> people watch less of each video (engagement problem). Growing watch hours -> video is becoming a more important communication channel. |

---

## 2. Engagement Metrics

### Engagement Score (`engagement_score`)

| | |
|---|---|
| **Source** | Brightcove Analytics API (pre-calculated by Brightcove) |
| **Type** | Float, 0-100 scale |
| **Definition** | The average percentage of the video that viewers watched. Brightcove divides each video into 100 equal segments, counts how many views reached each segment, and averages across all segments normalized by total views. |
| **Example** | A 10-minute video has `engagement_score = 65`. On average, viewers watched 6.5 minutes (65%) of the video. |
| **Business question** | How compelling is our content? Are viewers watching most of it or dropping off early? |
| **Action trigger** | Below 40 -> content may be too long, poorly structured, or mismatched with audience expectations. Above 70 -> strong content, use as a template for future production. Compare across channels to identify best practices. |

### Engagement Milestones (`video_engagement_1`, `_25`, `_50`, `_75`, `_100`)

| | |
|---|---|
| **Source** | Brightcove Analytics API |
| **Type** | Float -- **raw view counts, NOT percentages** |
| **Definition** | The number of views that reached each percentile of the video. `video_engagement_25 = 400` means 400 views made it to the 25% mark. |
| **Replay note** | Brightcove counts every pass through a percentile including replays and rewinds. If a viewer rewinds past the 25% mark and plays through it again, it counts again. This means values can exceed `video_view`. |
| **Example** | A video has: `video_view = 1,000`, `video_engagement_1 = 980`, `video_engagement_50 = 600`, `video_engagement_100 = 350`. Meaning: 98% started watching, 60% reached halfway, 35% finished. |
| **Business question** | Where exactly are viewers dropping off? |
| **Action trigger** | Big drop between 1% and 25% -> weak opening, viewers leave in the first quarter. Big drop between 75% and 100% -> consider shorter content or move the call-to-action earlier. |

**Converting to percentages (as done in the notebook):**
```sql
SUM(video_engagement_X) * 100.0 / NULLIF(SUM(video_view), 0)
```
Values above 100% indicate replay/autoplay behavior (e.g., Internet channel).

### Completion Rate (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from `video_engagement_100` and `video_view` |
| **Formula** | `SUM(video_engagement_100) * 100.0 / NULLIF(SUM(video_view), 0)` |
| **Definition** | Percentage of views that reached the end of the video. |
| **Example** | 1,000 views and 350 completions -> 35% completion rate. |
| **Business question** | Are viewers finishing our videos? |
| **Action trigger** | Below 25% -> content likely too long or loses relevance. Above 60% -> strong content. Compare by duration bucket to find optimal video length. |

### Halfway Rate (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from `video_engagement_50` and `video_view` |
| **Formula** | `SUM(video_engagement_50) * 100.0 / NULLIF(SUM(video_view), 0)` |
| **Definition** | Percentage of views that reached the midpoint of the video. |
| **Example** | 60% halfway rate means 6 out of 10 viewers make it to the middle. |
| **Business question** | Is the first half of our content holding attention? |
| **Action trigger** | Large gap between halfway and completion -> second half needs work. Small gap -> viewers who reach the middle tend to finish. |

### Percent Viewed (`video_percent_viewed`)

| | |
|---|---|
| **Source** | Brightcove Analytics API |
| **Type** | Float, 0-100 |
| **Definition** | Average percentage of the video watched per view. Similar to `engagement_score` but uses a simpler averaging method internally. |
| **Example** | `video_percent_viewed = 72` means on average viewers watched 72% of the video. |
| **Business question** | Quick proxy for content quality. |
| **Action trigger** | Use `engagement_score` for more accurate cross-video comparisons; this metric serves as a secondary reference. |

### Engagement Funnel Drop-off (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from engagement milestones |
| **Formula** | Difference between consecutive milestones, e.g., `started_pct - reached_25_pct` |
| **Definition** | The percentage point drop at each quarter of the video. Shows exactly where viewers are lost. |
| **Example** | Started: 95%, Reached 25%: 68%, Reached 50%: 52%. Biggest drop is 0-25% (27 points) -- the first quarter loses the most viewers. |
| **Business question** | Where exactly in the video are we losing viewers? |
| **Action trigger** | Largest drop at 0-25% -> strengthen opening hooks, front-load key messages. Largest drop at 75-100% -> move calls-to-action earlier. Different patterns per channel -> each channel needs different content guidelines. |

---

## 3. Conversion Metrics

### Play Rate (`play_rate` / calculated)

| | |
|---|---|
| **Source** | Brightcove Analytics API provides a per-record value; notebook also calculates an aggregate version |
| **API field** | `play_rate` (per video-day, pre-calculated by Brightcove) |
| **Notebook formula** | `SUM(video_view) * 100.0 / NULLIF(SUM(video_impression), 0)` |
| **Definition** | Percentage of impressions that converted into views. Of everyone who saw the video player, how many clicked play? |
| **Example** | 5,000 impressions, 1,500 views -> 30% play rate. 7 out of 10 people who saw the player chose not to watch. |
| **Business question** | Is our content compelling enough to click on? Are thumbnails and titles effective? |
| **Action trigger** | Below 20% -> thumbnail, title, or placement needs improvement. Above 50% -> strong conversion. Compare across channels to find which has the most clickable content. |

---

## 4. Device Metrics

### Device Views (`views_desktop`, `views_mobile`, `views_tablet`, `views_other`)

| | |
|---|---|
| **Source** | Brightcove Analytics API (via `device_type` dimension breakdown) |
| **Type** | Integer (daily count per video per device) |
| **Definition** | Views split by the device type used. The pipeline also captures `views_tv` and `views_connected_tv`, which are folded into `views_other` for analysis. |
| **Example** | desktop=800, mobile=150, tablet=40, other=10. Desktop dominates at 80%. |
| **Business question** | How are employees accessing video content? Should we invest in mobile optimization? |
| **Action trigger** | Mobile above 30% -> invest in mobile-first design (larger text, subtitles, vertical formats). Desktop-dominated -> ensure intranet video embeds work well on corporate devices. |

### Mobile Percentage (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from `views_mobile` and `video_view` |
| **Formula** | `SUM(views_mobile) * 100.0 / NULLIF(SUM(video_view), 0)` |
| **Definition** | Percentage of all views coming from mobile devices. Tracked monthly to identify trends. |
| **Example** | Mobile went from 12% in Jan 2024 to 22% in Dec 2025 -- a clear upward trend over 2 years. |
| **Business question** | Is mobile consumption growing? When should we switch to mobile-first? |
| **Action trigger** | Crossing 30% threshold -> justify budget for mobile optimization. Flat or declining -> desktop-first strategy remains appropriate. |

---

## 5. Content Metadata

### Video Duration (`video_duration`)

| | |
|---|---|
| **Source** | Brightcove CMS API |
| **Type** | Integer |
| **Unit** | **Milliseconds** (a 5-minute video = 300,000) |
| **Definition** | Total length of the video file as stored in Brightcove. |
| **Conversion** | / 1,000 = seconds, / 60,000 = minutes |
| **Example** | `video_duration = 420000` -> 420 seconds -> 7 minutes. |
| **Business question** | What video lengths perform best? Are we producing content at the right length? |
| **Action trigger** | Used in duration bucket analysis. If 1-3 min videos have 2x the completion rate of 20+ min videos, recommend shorter content for general communications. |

### Duration Buckets (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated via SQL CASE expression on `video_duration` |
| **Buckets** | 0-1 min (<=60,000ms), 1-3 min (<=180,000ms), 3-5 min (<=300,000ms), 5-10 min (<=600,000ms), 10-20 min (<=1,200,000ms), 20-30 min (<=1,800,000ms), 30+ min |
| **Example** | 3-5 min videos have 52% completion vs 18% for 30+ min -> 34 point penalty for long content. |
| **Business question** | What is the optimal video duration? |
| **Action trigger** | Share optimal duration guidelines with producers. Flag a "production mismatch" if we produce most videos at a non-optimal length. |

### Channel (`channel`)

| | |
|---|---|
| **Source** | Mapped from `account_id` during pipeline (via `config/accounts.json`) |
| **Definition** | The Brightcove account name, representing a business unit or content channel. |
| **Values** | Internet, Intranet, neo, research, research_internal, impact, circleone, fa_web, SuMiTrust, MyWay, digital_networks_events |
| **Categories** | internet_intranet (2), research (3), gwm (5), events (1) |
| **Business question** | Which channels deliver value? Where should we invest or consolidate? |
| **Action trigger** | Used in BCG matrix classification and cross-channel comparisons. |

### Content Age (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from `created_at` (Brightcove CMS API) |
| **Formula** | `DATEDIFF('day', MAX(created_at)::DATE, CURRENT_DATE)` |
| **Buckets** | <3 months, 3-6 months, 6-12 months, 1-2 years, 2+ years |
| **Example** | 60% of videos are older than 1 year. Engagement drops from 55% for new content to 32% for 2+ year old content. |
| **Business question** | Is our content library getting stale? Do newer videos perform better? |
| **Action trigger** | High proportion of old, low-engagement content -> recommend archiving or refreshing. |

### Days Since Last Viewed (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from `dt_last_viewed` (tracked by pipeline script 2) |
| **Formula** | `DATEDIFF('day', MAX(dt_last_viewed)::DATE, CURRENT_DATE)` |
| **Threshold** | >180 days = "stale" content |
| **Example** | A compliance video was last viewed 240 days ago despite being mandatory -- may indicate a process gap. |
| **Business question** | Which content has gone dormant? What should we archive? |
| **Action trigger** | Stale >180 days -> candidates for archiving (except compliance materials). Reduces storage costs and improves search relevance for active content. |

---

## 6. Composite / Strategic Metrics

### BCG Channel Classification (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated using median splits on `total_views` and `avg_engagement` per channel |
| **Definition** | Classifies each channel into a strategic quadrant based on whether it is above or below the median for reach (views) and quality (engagement). |
| **Quadrants** | |

| Quadrant | Reach | Engagement | Strategy |
|----------|-------|------------|----------|
| **Star** | High | High | Invest and scale |
| **Opportunity** | Low | High | Promote more -- audience loves it but few see it |
| **Cash Cow** | High | Low | Maintain, improve content quality |
| **Reconsider** | Low | Low | Consolidate or retire |

| | |
|---|---|
| **Example** | Intranet is a Cash Cow (massive reach, moderate engagement). A research channel is an Opportunity (small audience, highly engaged). |
| **Business question** | Where should we allocate resources across our 11 accounts? |
| **Action trigger** | Reconsider channels -> business case for consolidation (reduce licensing costs). Opportunities -> increase promotion. Stars -> protect and scale. |

### Year-over-Year Change (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated by comparing the two most recent complete calendar years |
| **Metrics compared** | Views (% change), impressions, watch hours, engagement (pp change), completion (pp change), play rate (pp change) |
| **Definition** | How each metric changed year-over-year. Volume metrics shown as percentage change; rate metrics as percentage point (pp) change. The current (incomplete) year is excluded from the comparison. |
| **Example** | Views grew +15.2% (2024: 850K -> 2025: 979K). Engagement dropped -2.3 pp (48.5% -> 46.2%). |
| **Business question** | Is our video platform growing or declining? Are we improving quality? |
| **Action trigger** | Views up but engagement down -> producing more but lower quality. Views down but engagement up -> smaller but more focused audience. Both declining -> urgent review needed. |

### Views Concentration (calculated in notebook)

| | |
|---|---|
| **Source** | Calculated from channel-level view totals |
| **Formula** | `channel_views / total_views * 100` |
| **Definition** | What percentage of all views comes from each channel. |
| **Example** | Intranet accounts for 78% of all views. The bottom 5 channels combined account for 3%. |
| **Business question** | Are we over-reliant on one channel? Is our reach diversified? |
| **Action trigger** | Extreme concentration (>70%) -> risk if that channel declines. Low-share channels -> evaluate whether to invest in growth or consolidate. |

---

## Quick Reference Table

### Brightcove API Fields (raw data)

| Field | API | Type | Unit | Notes |
|-------|-----|------|------|-------|
| `video_view` | Analytics | int | count | Per video per day |
| `video_impression` | Analytics | int | count | Player loads, not plays |
| `video_seconds_viewed` | Analytics | int | seconds | Total across all viewers |
| `engagement_score` | Analytics | float | 0-100% | Avg % of video watched |
| `video_engagement_1` | Analytics | float | **raw count** | Views reaching 1% mark |
| `video_engagement_25` | Analytics | float | **raw count** | Views reaching 25% mark |
| `video_engagement_50` | Analytics | float | **raw count** | Views reaching 50% mark |
| `video_engagement_75` | Analytics | float | **raw count** | Views reaching 75% mark |
| `video_engagement_100` | Analytics | float | **raw count** | Views reaching 100% mark |
| `video_percent_viewed` | Analytics | float | 0-100% | Avg % watched per view |
| `play_rate` | Analytics | float | 0-100% | views / impressions |
| `views_desktop` | Analytics | int | count | Desktop device views |
| `views_mobile` | Analytics | int | count | Mobile device views |
| `views_tablet` | Analytics | int | count | Tablet device views |
| `views_other` | Analytics | int | count | TV + connected TV + other |
| `video_duration` | CMS | int | **milliseconds** | Video file length |
| `created_at` | CMS | timestamp | ISO 8601 | When video was created |
| `name` | CMS | string | -- | Video title |
| `tags` | CMS | string | comma-separated | Content tags |
| `country` | CMS | string | -- | Custom field |
| `language` | CMS | string | -- | Custom field |
| `business_unit` | CMS | string | -- | Custom field |

### Pipeline-Generated Fields

| Field | Source | Notes |
|-------|--------|-------|
| `channel` | `config/accounts.json` | Account name mapped from `account_id` |
| `dt_last_viewed` | Pipeline script 2 | Last date any viewer watched the video |
| `data_type` | Pipeline | "year_YYYY" identifier |

### Notebook-Calculated Metrics

| Metric | Formula | Unit |
|--------|---------|------|
| Watch hours | `SUM(video_seconds_viewed) / 3600` | hours |
| Completion rate | `SUM(video_engagement_100) * 100 / SUM(video_view)` | % |
| Halfway rate | `SUM(video_engagement_50) * 100 / SUM(video_view)` | % |
| Play rate (agg) | `SUM(video_view) * 100 / SUM(video_impression)` | % |
| Mobile % | `SUM(views_mobile) * 100 / SUM(video_view)` | % |
| Duration (min) | `video_duration / 60000` | minutes |
| Content age | `DATEDIFF('day', created_at, CURRENT_DATE)` | days |
| Days since viewed | `DATEDIFF('day', dt_last_viewed, CURRENT_DATE)` | days |
| Funnel milestones | `SUM(video_engagement_X) * 100 / SUM(video_view)` | % |
| YoY change (volume) | `(curr - prev) / prev * 100` | % change |
| YoY change (rates) | `curr - prev` | pp (percentage points) |
| BCG classification | Median split on views x engagement | category |

---

## Common Pitfalls

**1. video_engagement fields are NOT percentages**
They are raw view counts at each percentile. `video_engagement_50 = 600` means 600 views
reached the halfway mark, not 600%. Always divide by `SUM(video_view)` and multiply by 100
to get a percentage.

**2. video_duration is in milliseconds, not seconds**
A 5-minute video = 300,000 (not 300). Divide by 1,000 for seconds, 60,000 for minutes.

**3. Engagement milestones can exceed video_view**
Brightcove counts every pass through a percentile including replays and rewinds. Channels
with autoplay or looping behavior (e.g., Internet) can show "started" values above 100%.
The notebook auto-detects and excludes these from the aggregate funnel.

**4. Unweighted AVG vs weighted SUM/SUM**
`AVG(engagement_score)` gives equal weight to every video-date row, regardless of view
count. A video with 2 views counts the same as one with 20,000 views. For aggregate
metrics like completion rate, the notebook uses `SUM(field) / SUM(video_view)` to properly
weight by actual viewership.

**5. Current incomplete year and month**
The notebook excludes the current incomplete month from monthly trends and the current
incomplete year from YoY comparisons. Comparing January 2026 against full-year 2025
would be misleading.
