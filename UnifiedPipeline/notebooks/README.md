# Video Analytics Notebooks & Reports

This directory contains analytics tools for both operational reporting and interview preparation.

## Available Tools

### 1. **Executive Video Analytics** (`executive_video_analytics.ipynb`)
- **Purpose:** Operational dashboard for regular video performance analysis
- **Audience:** Communication teams, data scientists, executives
- **Focus:** Current performance, top content, device trends

### 2. **Business Impact Discovery** (`business_impact_discovery.ipynb`) 🆕
- **Purpose:** Interview preparation - find concrete business impact examples
- **Audience:** Analytics professionals preparing for interviews
- **Focus:** ROI, strategic decisions, quantifiable business value

### 3. **Quick Report Generator** (`../scripts/generate_interview_report.py`) 🆕
- **Purpose:** Command-line report generation without Jupyter
- **Output:** Text-based summary with key metrics for interviews

### 4. **Business Impact Guide** (`BUSINESS_IMPACT_GUIDE.md`) 🆕
- **Purpose:** How-to guide for translating analytics into interview responses
- **Content:** STAR templates, talking points, metric checklists

---

## Quick Start

### For Regular Analytics Work:

1. **Complete the data pipeline** (scripts 1-3 or migration to DuckDB)
2. **Install dependencies:**
   ```bash
   pip install jupyterlab duckdb pandas matplotlib seaborn
   ```
3. **Launch Jupyter:**
   ```bash
   cd UnifiedPipeline
   jupyter lab notebooks/executive_video_analytics.ipynb
   ```

### For Interview Preparation:

**Option A: Interactive Notebook (Recommended)**
```bash
cd UnifiedPipeline
jupyter lab notebooks/business_impact_discovery.ipynb
# Run all cells, review findings in each section
```

**Option B: Quick Command-Line Report**
```bash
cd UnifiedPipeline
python scripts/generate_interview_report.py
# Or save to file:
python scripts/generate_interview_report.py --output my_report.txt
```

**Option C: Focus on Recent Data**
```bash
python scripts/generate_interview_report.py --date-filter 2024-01-01 --output 2024_report.txt
```

---

## Executive Video Analytics Notebook

## Notebook Structure

The notebook is organized into 9 sections, each targeting specific business questions.

---

## Section 1: Executive Summary

**Cell: Setup and Imports**
- Connects to the DuckDB database
- Loads required Python libraries
- Confirms database connection and size

**Cell: Key Performance Indicators**
- Displays high-level metrics at a glance

| Metric | What It Tells You |
|--------|-------------------|
| Total Videos Tracked | Size of your video library |
| Total Channels/Accounts | Number of distribution channels |
| Total Video Views | Overall reach and consumption |
| Avg Engagement Score | How engaged viewers are (0-100%) |
| Avg Percent Viewed | How much of videos people watch |
| Data Period | Time range covered by the data |

**Business Questions Answered:**
- How large is our video operation?
- What's our overall performance baseline?

**Cell: Monthly View Trends**
- Bar chart showing views per month
- Line chart showing engagement trends over time

**Business Questions Answered:**
- Are video views growing or declining?
- Is engagement improving over time?
- Are there seasonal patterns?

---

## Section 2: Top Performing Content

**Cell: Top 20 Videos by Total Views**
- Ranks videos by total lifetime views
- Shows channel, video name, views, engagement, and completion rate

**Business Questions Answered:**
- What content resonates most with our audience?
- Which videos should we use as templates for future content?

**Cell: Top 20 Videos by Engagement**
- Ranks videos by engagement score (minimum 100 views for statistical validity)
- Shows completion rate and percentage who watched to 100%

**Business Questions Answered:**
- Which videos keep viewers most engaged?
- What content holds attention best (regardless of view count)?
- Are high-view videos also high-engagement, or is there a disconnect?

---

## Section 3: Engagement Analysis

**Cell: Engagement Funnel**
- Horizontal bar chart showing viewer progression through videos
- Tracks: Started → 25% → 50% → 75% → Completed

**Metrics Explained:**
| Metric | Meaning |
|--------|---------|
| video_engagement_1 | % who started watching |
| video_engagement_25 | % who reached 25% of video |
| video_engagement_50 | % who reached halfway |
| video_engagement_75 | % who reached 75% |
| video_engagement_100 | % who watched to the end |

**Business Questions Answered:**
- Where do we lose viewers?
- What's our biggest drop-off point?
- What percentage of viewers complete our videos?

**Actionable Insight:** If the biggest drop-off is in the first 25%, focus on improving video openings (thumbnails, intros, hooks).

---

## Section 4: Content Strategy Analysis

**Cell: Video Length vs Engagement**
- Analyzes performance by video duration buckets
- Two charts: Total views by duration, Completion rate by duration

**Duration Buckets:**
- Under 1 min
- 1-3 min
- 3-5 min
- 5-10 min
- 10-20 min
- 20-30 min
- Over 30 min

**Business Questions Answered:**
- What's the optimal video length for our audience?
- Do shorter videos get more completion?
- Are longer videos worth the production investment?

**Cell: Content Type Performance**
- Breaks down performance by content type (if classified)
- Shows number of videos, views, engagement, and completion

**Business Questions Answered:**
- Which content types perform best?
- Should we produce more of certain content types?
- Are there content types with high engagement but low volume (opportunity)?

---

## Section 5: Channel/Account Performance

**Cell: Channel Comparison**
- Horizontal bar charts comparing all channels
- Shows total views and average engagement by channel

**Business Questions Answered:**
- Which channels drive the most views?
- Which channels have the most engaged audiences?
- Are there high-engagement channels that are under-promoted?
- How should we allocate resources across channels?

**Insight:** Look for channels with high engagement but low views - these are opportunities to increase promotion.

---

## Section 6: Device & Platform Analysis

**Cell: Device Breakdown**
- Pie chart showing distribution of views by device
- Bar chart showing absolute numbers

**Device Categories:**
- Desktop (computer/laptop)
- Mobile (smartphones)
- Tablet
- Other

**Business Questions Answered:**
- How are viewers accessing our content?
- Do we need to optimize for mobile?
- Should we consider vertical video formats?

**Cell: Device Trends Over Time**
- Line chart showing desktop vs mobile percentage over time

**Business Questions Answered:**
- Is mobile viewership growing?
- Do we need to shift our production approach?

---

## Section 7: Content Lifecycle Analysis

**Cell: Stale Content Alert**
- Lists videos not viewed in 180+ days
- Sorted by historical views (shows content that once had audience interest)

**Business Questions Answered:**
- What content is sitting unused in our library?
- Should we archive, update, or re-promote stale content?
- Are there evergreen topics that need refreshed versions?

**Cell: Recent Content Performance (Last 30 Days)**
- Shows top performing videos from the past month
- Indicates what's currently resonating

**Business Questions Answered:**
- What content is working right now?
- Are recent uploads performing well?
- What topics should we double down on?

---

## Section 8: Actionable Insights & Recommendations

**Cell: Auto-Generated Insights**

This cell analyzes the data and generates specific recommendations:

| Insight | What It Tells You |
|---------|-------------------|
| **Optimal Video Length** | Which duration has highest completion rate |
| **Underutilized Channels** | High-engagement channels with low views (promote more) |
| **Mobile Optimization** | Whether mobile-first strategy is needed |
| **Engagement Optimization** | Where viewers drop off and how to fix it |
| **Content Freshness** | Whether content library needs cleanup |

**Business Questions Answered:**
- What should we do differently?
- Where are the quick wins?
- What's our content strategy priority?

---

## Section 9: Custom Query Playground

**Cell: Custom Query**
- Pre-loaded example: Find videos with high impressions but low play rate
- These videos may need better thumbnails or titles

**How to Use:**
1. Modify the SQL query in the cell
2. Run the cell to see results
3. Export results if needed

**Example Queries You Can Try:**

```sql
-- Videos by specific channel
SELECT * FROM daily_analytics
WHERE channel = 'Internet' AND date >= '2026-01-01'
LIMIT 100

-- Weekly trends
SELECT
    DATE_TRUNC('week', date) as week,
    SUM(video_view) as views
FROM daily_analytics
GROUP BY 1 ORDER BY 1

-- Videos created in last 90 days and their performance
SELECT
    video_id, MAX(name), SUM(video_view),
    MAX(created_at)::DATE as created
FROM daily_analytics
WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY video_id
ORDER BY SUM(video_view) DESC
```

**Cell: Schema Reference**
- Lists all available columns in the database
- Use this as a reference when writing custom queries

---

## Key Metrics Glossary

| Metric | Definition | Good Value |
|--------|------------|------------|
| `video_view` | Number of times video was played | Higher is better |
| `video_impression` | Number of times video was displayed | Higher reach |
| `play_rate` | views / impressions | >5% is good |
| `engagement_score` | Brightcove's engagement metric | >50% is good |
| `video_percent_viewed` | Average % of video watched | >50% is good |
| `video_engagement_100` | % who watched entire video | >20% is good |

---

## Tips for Executives

1. **Start with Section 1** for the high-level picture
2. **Jump to Section 8** for actionable recommendations
3. **Use Section 2** when you need to highlight success stories
4. **Check Section 7** monthly to clean up stale content
5. **Share Section 5** with channel owners for accountability

## Tips for Data Scientists

1. **Use Section 9** for ad-hoc analysis
2. **Modify queries** to drill into specific questions
3. **Export DataFrames** to CSV for further analysis:
   ```python
   df.to_csv('export.csv', index=False)
   ```
4. **Add new cells** for custom visualizations

## Tips for Communication Specialists

1. **Section 4** tells you what content formats work
2. **Section 3** shows where to focus editing efforts (reduce drop-off)
3. **Section 6** informs production decisions (mobile optimization)
4. **Top performers in Section 2** are templates for future content

---

## Troubleshooting

**"Database not found" error:**
- Ensure you've run the data pipeline (scripts 1-3)
- Check that `output/analytics.duckdb` exists

**Empty charts or no data:**
- Verify the date range in your data
- Check if filters are too restrictive

**Slow performance:**
- The notebook reads from DuckDB which is fast
- If slow, check your disk I/O or database size

---

## Updating the Analysis

Run this notebook regularly (weekly/monthly) after updating the data pipeline:

```bash
# Update data
python scripts/1_cms_metadata.py
python scripts/2_dt_last_viewed.py
python scripts/3_daily_analytics.py

# Then re-run the notebook
jupyter lab notebooks/executive_video_analytics.ipynb
```

The notebook always reads from the latest data in `analytics.duckdb`.
