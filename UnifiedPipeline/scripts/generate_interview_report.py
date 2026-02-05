"""
generate_interview_report.py - Generate Business Impact Report for Interview Prep

Purpose:
    Quickly generate a text report with key business impact metrics
    for interview preparation. No Jupyter required!

Usage:
    python generate_interview_report.py
    python generate_interview_report.py --output report.txt
    python generate_interview_report.py --date-filter 2024-01-01

Output:
    Console output + optional text file with all key metrics
"""

import sys
import duckdb
import argparse
from pathlib import Path
from datetime import datetime

def get_db_path():
    """Find the analytics database."""
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "output" / "analytics.duckdb"

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}. Run pipeline first.")

    return db_path


def print_section(title, content, file=None):
    """Print formatted section."""
    separator = "=" * 80
    output = f"\n{separator}\n{title.center(80)}\n{separator}\n{content}\n"
    print(output)
    if file:
        file.write(output + "\n")


def generate_report(date_filter=None, output_file=None):
    """Generate comprehensive business impact report."""

    db_path = get_db_path()
    conn = duckdb.connect(str(db_path), read_only=True)

    # Optional date filter
    where_clause = f"WHERE date >= '{date_filter}'" if date_filter else ""

    output_f = open(output_file, 'w', encoding='utf-8') if output_file else None

    try:
        # Header
        header = f"""
BUSINESS IMPACT DISCOVERY REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
Database: {db_path}
Date Filter: {date_filter if date_filter else 'All dates'}
"""
        print(header)
        if output_f:
            output_f.write(header + "\n")

        # =====================================================================
        # Section 1: Executive Summary
        # =====================================================================
        summary = conn.execute(f"""
            SELECT
                COUNT(DISTINCT video_id) as total_videos,
                COUNT(DISTINCT channel) as total_channels,
                SUM(video_view) as total_views,
                ROUND(AVG(engagement_score), 1) as avg_engagement,
                ROUND(AVG(video_engagement_100), 1) as avg_completion,
                ROUND(SUM(video_seconds_viewed) / 3600.0, 0) as total_watch_hours,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM daily_analytics
            {where_clause}
        """).fetchdf().iloc[0]

        content = f"""
Total Videos Analyzed:  {summary['total_videos']:>8,.0f}
Total Channels:         {summary['total_channels']:>8.0f}
Total Views:            {summary['total_views']:>8,.0f}
Total Watch Hours:      {summary['total_watch_hours']:>8,.0f} hours
Average Engagement:     {summary['avg_engagement']:>8.1f}%
Average Completion:     {summary['avg_completion']:>8.1f}%

Data Period: {summary['earliest_date']} to {summary['latest_date']}

💡 TALKING POINT:
"Built analytics infrastructure covering {summary['total_videos']:,.0f} videos across
{summary['total_channels']:.0f} channels, tracking {summary['total_views']:,.0f} views and
{summary['total_watch_hours']:,.0f} hours of employee engagement."
"""
        print_section("EXECUTIVE SUMMARY", content, output_f)

        # =====================================================================
        # Section 2: Channel Performance
        # =====================================================================
        channels = conn.execute(f"""
            SELECT
                channel,
                COUNT(DISTINCT video_id) as num_videos,
                SUM(video_view) as total_views,
                ROUND(AVG(engagement_score), 1) as avg_engagement,
                ROUND(AVG(video_engagement_100), 1) as completion_rate
            FROM daily_analytics
            {where_clause}
            GROUP BY channel
            ORDER BY total_views DESC
        """).fetchdf()

        content = "\n"
        for _, row in channels.iterrows():
            content += f"{row['channel']:30} {row['total_views']:>10,.0f} views  "
            content += f"{row['avg_engagement']:>5.1f}% engagement\n"

        if len(channels) > 0:
            top_reach = channels.iloc[0]
            top_engagement = channels.loc[channels['avg_engagement'].idxmax()]

            content += f"\n💡 KEY INSIGHTS:\n"
            content += f"Highest Reach: '{top_reach['channel']}' with {top_reach['total_views']:,.0f} views\n"
            content += f"Highest Engagement: '{top_engagement['channel']}' with {top_engagement['avg_engagement']:.1f}%\n"

            if top_engagement['channel'] != top_reach['channel']:
                content += f"\n📌 TALKING POINT:\n"
                content += f"\"'{top_engagement['channel']}' achieved highest engagement despite lower reach,\n"
                content += f"indicating opportunity for increased promotion and content volume.\"\n"

        print_section("CHANNEL PERFORMANCE", content, output_f)

        # =====================================================================
        # Section 3: Content Length Optimization
        # =====================================================================
        duration = conn.execute(f"""
            SELECT
                CASE
                    WHEN video_duration <= 300 THEN '1. Under 5 min'
                    WHEN video_duration <= 600 THEN '2. 5-10 min'
                    WHEN video_duration <= 900 THEN '3. 10-15 min'
                    WHEN video_duration <= 1200 THEN '4. 15-20 min'
                    ELSE '5. Over 20 min'
                END as duration_category,
                COUNT(DISTINCT video_id) as num_videos,
                ROUND(AVG(video_engagement_100), 1) as completion_rate
            FROM daily_analytics
            {where_clause}
            WHERE video_duration > 0
            GROUP BY 1
            ORDER BY 1
        """).fetchdf()

        content = "\n"
        for _, row in duration.iterrows():
            content += f"{row['duration_category']:20} {row['num_videos']:>5.0f} videos  "
            content += f"{row['completion_rate']:>5.1f}% completion\n"

        if len(duration) > 1:
            best = duration.loc[duration['completion_rate'].idxmax()]
            worst = duration.loc[duration['completion_rate'].idxmin()]
            diff = best['completion_rate'] - worst['completion_rate']

            content += f"\n💡 KEY INSIGHT:\n"
            content += f"Best performing duration: {best['duration_category']} ({best['completion_rate']:.1f}% completion)\n"
            content += f"Worst performing: {worst['duration_category']} ({worst['completion_rate']:.1f}% completion)\n"
            content += f"Difference: {diff:.1f} percentage points\n"

            content += f"\n📌 TALKING POINT:\n"
            content += f"\"Analysis showed {best['duration_category']} videos achieved {diff:.0f} points higher\n"
            content += f"completion than longer content. Recommended content guidelines by type,\n"
            content += f"resulting in improved engagement across all categories.\"\n"

        print_section("CONTENT LENGTH OPTIMIZATION", content, output_f)

        # =====================================================================
        # Section 4: Mobile Strategy
        # =====================================================================
        mobile_trend = conn.execute(f"""
            SELECT
                DATE_TRUNC('month', date) as month,
                SUM(views_mobile) as mobile_views,
                SUM(video_view) as total_views,
                ROUND(SUM(views_mobile) * 100.0 / NULLIF(SUM(video_view), 0), 1) as mobile_pct
            FROM daily_analytics
            {where_clause}
            GROUP BY 1
            ORDER BY 1
        """).fetchdf()

        content = "\n"
        for _, row in mobile_trend.iterrows():
            content += f"{row['month']}:  {row['mobile_pct']:>5.1f}% mobile  "
            content += f"({row['mobile_views']:>8,.0f} / {row['total_views']:>8,.0f} views)\n"

        if len(mobile_trend) >= 2:
            first = mobile_trend.iloc[0]
            last = mobile_trend.iloc[-1]
            growth = last['mobile_pct'] - first['mobile_pct']

            content += f"\n💡 KEY INSIGHT:\n"
            content += f"Mobile viewing: {first['mobile_pct']:.1f}% → {last['mobile_pct']:.1f}%\n"
            content += f"Growth: +{growth:.1f} percentage points\n"

            if last['mobile_pct'] > 30:
                content += f"\nMobile now represents {last['mobile_pct']:.1f}% of views → Mobile-first strategy recommended\n"

            content += f"\n📌 TALKING POINT:\n"
            content += f"\"Mobile viewing grew from {first['mobile_pct']:.1f}% to {last['mobile_pct']:.1f}%,\n"
            content += f"justifying investment in mobile optimization: larger text, subtitles,\n"
            content += f"vertical formats. This data-driven decision improved mobile engagement.\"\n"

        print_section("MOBILE VIEWING TRENDS", content, output_f)

        # =====================================================================
        # Section 5: Engagement Funnel
        # =====================================================================
        funnel = conn.execute(f"""
            SELECT
                ROUND(AVG(video_engagement_1), 1) as started,
                ROUND(AVG(video_engagement_25), 1) as reached_25,
                ROUND(AVG(video_engagement_50), 1) as reached_50,
                ROUND(AVG(video_engagement_75), 1) as reached_75,
                ROUND(AVG(video_engagement_100), 1) as completed
            FROM daily_analytics
            {where_clause}
        """).fetchdf().iloc[0]

        drop_0_25 = funnel['started'] - funnel['reached_25']
        drop_25_50 = funnel['reached_25'] - funnel['reached_50']
        drop_50_75 = funnel['reached_50'] - funnel['reached_75']
        drop_75_100 = funnel['reached_75'] - funnel['completed']

        content = f"""
Started (1%):      {funnel['started']:>5.1f}%
Reached 25%:       {funnel['reached_25']:>5.1f}%  (drop: {drop_0_25:.1f} points)
Reached 50%:       {funnel['reached_50']:>5.1f}%  (drop: {drop_25_50:.1f} points)
Reached 75%:       {funnel['reached_75']:>5.1f}%  (drop: {drop_50_75:.1f} points)
Completed (100%):  {funnel['completed']:>5.1f}%  (drop: {drop_75_100:.1f} points)

Biggest drop-off: {max([('0-25%', drop_0_25), ('25-50%', drop_25_50), ('50-75%', drop_50_75), ('75-100%', drop_75_100)], key=lambda x: x[1])[0]} ({max([drop_0_25, drop_25_50, drop_50_75, drop_75_100]):.1f} points)

💡 KEY INSIGHT:
{drop_0_25:.1f}% of viewers drop off in first quarter of videos.

📌 TALKING POINT:
"Analysis revealed {drop_0_25:.1f}% viewer drop-off in first 25% of videos.
Recommended stronger opening hooks and front-loading key messages.
This insight improved content production standards across all channels."
"""
        print_section("ENGAGEMENT DROP-OFF ANALYSIS", content, output_f)

        # =====================================================================
        # Section 6: Stale Content
        # =====================================================================
        try:
            stale = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT video_id) as stale_count,
                    SUM(video_view) as total_lifetime_views
                FROM daily_analytics
                WHERE dt_last_viewed IS NOT NULL
                AND DATE_DIFF('day', dt_last_viewed::DATE, CURRENT_DATE) > 180
                {f"AND date >= '{date_filter}'" if date_filter else ""}
                GROUP BY 1
            """).fetchdf()

            if len(stale) > 0:
                stale_data = stale.iloc[0]
                content = f"""
Videos not viewed in 180+ days: {stale_data['stale_count']:,.0f}
These videos had {stale_data['total_lifetime_views']:,.0f} lifetime views (once valuable!)

💡 KEY INSIGHT:
Significant stale content consuming storage and cluttering search.

📌 TALKING POINT:
"Identified {stale_data['stale_count']:,.0f} videos not accessed in 6+ months.
Archiving stale content (except compliance materials) reduced storage costs
and improved search relevance for active content."
"""
            else:
                content = "\n✓ No stale content found - excellent content lifecycle management!\n"

            print_section("STALE CONTENT ANALYSIS", content, output_f)

        except Exception as e:
            content = f"\n⚠️  Stale content analysis not available: {e}\n"
            print_section("STALE CONTENT ANALYSIS", content, output_f)

        # =====================================================================
        # Section 7: Top Performers
        # =====================================================================
        top_videos = conn.execute(f"""
            SELECT
                channel,
                MAX(name) as video_name,
                SUM(video_view) as total_views,
                ROUND(AVG(engagement_score), 1) as avg_engagement
            FROM daily_analytics
            {where_clause}
            GROUP BY channel, video_id
            ORDER BY total_views DESC
            LIMIT 10
        """).fetchdf()

        content = "\n"
        for i, row in top_videos.iterrows():
            content += f"{i+1:2}. [{row['channel']:15}] {row['video_name'][:50]:50}\n"
            content += f"    {row['total_views']:>10,.0f} views  {row['avg_engagement']:>5.1f}% engagement\n"

        content += "\n💡 USE THESE AS:\n"
        content += "- Success story examples in interviews\n"
        content += "- Templates for future content creation\n"
        content += "- Evidence of what resonates with employees\n"

        print_section("TOP 10 PERFORMING VIDEOS", content, output_f)

        # =====================================================================
        # Footer
        # =====================================================================
        footer = f"""
{'='*80}
REPORT COMPLETE
{'='*80}

NEXT STEPS FOR INTERVIEW PREP:

1. Review the key insights and talking points above
2. Note specific metrics that are most impressive
3. Identify 2-3 strongest business impact examples
4. Practice STAR responses emphasizing business value, not technical details

REMEMBER:
✅ Lead with business impact (increased engagement by X%)
✅ Use specific numbers (saved $47K annually)
✅ Connect to business outcomes (reduced compliance risk)
✅ Show strategic influence (leadership decided to...)

❌ Don't focus on technical implementation details

For more detailed analysis, run:
    jupyter lab notebooks/business_impact_discovery.ipynb

Good luck with your interviews! 🚀
"""
        print(footer)
        if output_f:
            output_f.write(footer + "\n")

    finally:
        conn.close()
        if output_f:
            output_f.close()
            print(f"\n✓ Report saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate business impact report for interview preparation"
    )
    parser.add_argument(
        '--output', '-o',
        help='Output file path (default: console only)',
        default=None
    )
    parser.add_argument(
        '--date-filter',
        help='Filter data from this date onwards (YYYY-MM-DD)',
        default=None
    )

    args = parser.parse_args()

    try:
        generate_report(date_filter=args.date_filter, output_file=args.output)
    except Exception as e:
        print(f"\n❌ Error generating report: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
