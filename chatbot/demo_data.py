"""
Enhanced demo data for showcasing the Video Analytics Intelligence platform.

Includes realistic financial services data with business divisions, regions,
and year-over-year quarterly comparisons.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Seed for reproducibility
random.seed(42)
np.random.seed(42)

# =============================================================================
# ORGANIZATIONAL STRUCTURE
# =============================================================================

BUSINESS_DIVISIONS = {
    "IB": "Investment Bank",
    "GWM": "Global Wealth Management",
    "AM": "Asset Management",
    "P&C": "Personal & Corporate Banking",
    "GF": "Group Functions",
}

REGIONS = ["APAC", "EMEA", "Americas"]

CHANNELS = {
    "IB": ["Markets", "Investment Banking Advisory", "Research"],
    "GWM": ["Wealth Management", "Wealth Planning", "Client Advisory"],
    "AM": ["Asset Management", "Sustainable Investing", "Index Solutions"],
    "P&C": ["Personal Banking", "Corporate Banking", "Digital Services"],
    "GF": ["Corporate Communications", "HR & Training", "Technology", "Compliance"],
}

CONTENT_TYPES = [
    "Quarterly Results",
    "Training",
    "Product Demo",
    "Thought Leadership",
    "Client Communication",
    "Internal Update",
    "Compliance",
    "Event Recording",
]

# =============================================================================
# REALISTIC VIDEO TITLES BY CATEGORY
# =============================================================================

VIDEO_TEMPLATES = {
    "Quarterly Results": [
        "{quarter} {year} Results: Group Performance Overview",
        "{quarter} {year} Earnings Call Highlights",
        "{quarter} {year} {division} Division Results",
        "{quarter} {year} Regional Performance: {region}",
        "{quarter} {year} Financial Results Webcast",
        "{quarter} {year} Investor Presentation",
    ],
    "Training": [
        "Compliance Training: Anti-Money Laundering {year}",
        "New Employee Onboarding: {division}",
        "Leadership Development Module {num}",
        "Client Relationship Management Training",
        "Risk Management Fundamentals",
        "Digital Tools Training: {year} Updates",
        "Cybersecurity Awareness {year}",
        "Regulatory Updates for {region}",
    ],
    "Product Demo": [
        "Mobile Banking App Walkthrough",
        "Trading Platform Demo: New Features",
        "Portfolio Analytics Tool Overview",
        "Client Portal: Getting Started",
        "Research Platform Tutorial",
        "Digital Onboarding Experience",
    ],
    "Thought Leadership": [
        "Market Outlook {year}: Global Perspectives",
        "ESG Investing: Trends and Opportunities",
        "Sustainable Finance Overview",
        "{region} Economic Outlook {year}",
        "Private Markets Investment Strategies",
        "Digital Assets: Institutional Perspective",
        "Interest Rate Environment Analysis",
    ],
    "Client Communication": [
        "Investment Strategy Update: {region}",
        "Market Commentary: {quarter} {year}",
        "Portfolio Review Guidelines",
        "Client Event Highlights: {region}",
        "Wealth Planning Insights",
    ],
    "Internal Update": [
        "CEO Town Hall: {quarter} {year}",
        "Strategy Update: {division}",
        "{region} Leadership Message",
        "Annual Review {year}",
        "Innovation Showcase {year}",
        "Cultural Initiatives Update",
    ],
    "Compliance": [
        "Code of Conduct Training {year}",
        "Data Privacy Requirements",
        "Cross-Border Regulations: {region}",
        "Conflicts of Interest Guidelines",
        "Information Barriers Training",
    ],
    "Event Recording": [
        "Investor Day {year}: Full Recording",
        "Annual General Meeting {year}",
        "{region} Client Summit Highlights",
        "Technology Conference Keynote",
        "Sustainability Forum {year}",
    ],
}

# =============================================================================
# DATA GENERATION
# =============================================================================

def generate_video_title(content_type, division, region, year, quarter_num):
    """Generate a realistic video title."""
    templates = VIDEO_TEMPLATES.get(content_type, VIDEO_TEMPLATES["Internal Update"])
    template = random.choice(templates)

    quarters = ["Q1", "Q2", "Q3", "Q4"]
    quarter = quarters[quarter_num % 4]

    return template.format(
        quarter=quarter,
        year=year,
        division=BUSINESS_DIVISIONS.get(division, division),
        region=region,
        num=random.randint(1, 5)
    )


def generate_demo_dimensions(num_videos=80):
    """Generate video metadata with business divisions and regions."""
    data = []
    divisions = list(BUSINESS_DIVISIONS.keys())

    # Generate quarterly results videos for YoY comparison
    for year in [2024, 2025]:
        for q in range(4):
            for div in ["GF", "IB", "GWM"]:  # Main divisions doing quarterly updates
                for region in REGIONS:
                    if random.random() < 0.6:  # Not all combinations exist
                        duration_sec = random.randint(1200, 3600)  # 20-60 min
                        video_id = f"vid_{len(data):04d}"
                        data.append({
                            "video_id": video_id,
                            "name": generate_video_title("Quarterly Results", div, region, year, q),
                            "division": div,
                            "division_name": BUSINESS_DIVISIONS[div],
                            "region": region,
                            "channel": random.choice(CHANNELS[div]),
                            "video_content_type": "Quarterly Results",
                            "video_duration": duration_sec * 1000,
                            "video_duration_seconds": duration_sec,
                            "created_at": datetime(year, (q * 3) + 2, 15) + timedelta(days=random.randint(0, 10)),
                            "published_at": datetime(year, (q * 3) + 2, 20) + timedelta(days=random.randint(0, 5)),
                            "language": "English",
                            "year": year,
                            "quarter": f"Q{q+1}",
                        })

    # Generate other content types
    remaining = num_videos - len(data)
    other_content_types = [ct for ct in CONTENT_TYPES if ct != "Quarterly Results"]

    for i in range(remaining):
        division = random.choice(divisions)
        region = random.choice(REGIONS)
        content_type = random.choice(other_content_types)
        year = random.choice([2024, 2025, 2025, 2025, 2026])  # Weight toward recent

        # Duration varies by content type
        if content_type == "Training":
            duration_sec = random.randint(600, 2400)  # 10-40 min
        elif content_type == "Product Demo":
            duration_sec = random.randint(180, 600)  # 3-10 min
        elif content_type == "Thought Leadership":
            duration_sec = random.randint(300, 1200)  # 5-20 min
        elif content_type == "Event Recording":
            duration_sec = random.randint(2400, 5400)  # 40-90 min
        else:
            duration_sec = random.randint(180, 900)  # 3-15 min

        video_id = f"vid_{len(data):04d}"
        data.append({
            "video_id": video_id,
            "name": generate_video_title(content_type, division, region, year, random.randint(0, 3)),
            "division": division,
            "division_name": BUSINESS_DIVISIONS[division],
            "region": region,
            "channel": random.choice(CHANNELS[division]),
            "video_content_type": content_type,
            "video_duration": duration_sec * 1000,
            "video_duration_seconds": duration_sec,
            "created_at": datetime(year, random.randint(1, 12), random.randint(1, 28)),
            "published_at": datetime(year, random.randint(1, 12), random.randint(1, 28)),
            "language": random.choice(["English", "German", "French", "Mandarin"]),
            "year": year,
            "quarter": f"Q{random.randint(1, 4)}",
        })

    return pd.DataFrame(data)


def generate_demo_facts(dimensions_df, rows_per_video=40):
    """Generate daily analytics data with realistic patterns.

    KEY STORY FOR DEMO: Clear negative correlation between duration and completion.
    - Short videos (< 5 min): ~70-85% completion
    - Medium videos (5-15 min): ~45-65% completion
    - Long videos (> 15 min): ~25-45% completion
    - Exception: Training/Compliance videos maintain high completion (mandatory viewing)
    """
    data = []

    for _, video in dimensions_df.iterrows():
        video_id = video["video_id"]
        published = video["published_at"]
        content_type = video["video_content_type"]
        division = video["division"]
        duration_min = video["video_duration_seconds"] / 60

        # Base views vary by content type and division
        if content_type == "Quarterly Results":
            base_views = random.randint(500, 5000)
        elif content_type == "Training":
            base_views = random.randint(100, 1000)
        elif division == "GWM":
            base_views = random.randint(200, 2000)
        else:
            base_views = random.randint(50, 800)

        # CRITICAL: Engagement based on duration - creates the compelling correlation story
        # Training and Compliance are exceptions (mandatory viewing = high completion)
        if content_type in ["Training", "Compliance"]:
            # Mandatory content: high completion regardless of duration
            engagement_base = random.uniform(0.70, 0.90)
        else:
            # Duration-based engagement - THE KEY INSIGHT for the demo
            if duration_min <= 3:
                # Ultra-short: highest completion
                engagement_base = random.uniform(0.75, 0.88)
            elif duration_min <= 5:
                # Sweet spot: very high completion
                engagement_base = random.uniform(0.68, 0.82)
            elif duration_min <= 10:
                # Medium: good completion
                engagement_base = random.uniform(0.50, 0.65)
            elif duration_min <= 20:
                # Getting long: noticeable drop
                engagement_base = random.uniform(0.35, 0.50)
            elif duration_min <= 40:
                # Long: significant drop
                engagement_base = random.uniform(0.22, 0.38)
            else:
                # Very long (event recordings, etc.): lowest completion
                engagement_base = random.uniform(0.12, 0.25)

        # Generate daily data points
        num_days = random.randint(20, rows_per_video)
        for day_offset in range(num_days):
            date = published + timedelta(days=day_offset)
            if date > datetime(2026, 1, 15):
                continue

            # Views decay over time (power law)
            decay = 1.0 / (1 + day_offset * 0.1)
            daily_views = max(1, int(base_views * decay * random.uniform(0.5, 1.5)))

            # Engagement funnel - directly derived from engagement_base for clear story
            # eng_100 (completion) is the key metric - derived directly from duration-based engagement_base
            eng_100 = round(engagement_base * random.uniform(0.92, 1.08), 4)  # Small variance around base
            eng_100 = max(0.08, min(0.95, eng_100))  # Keep in valid range

            # Work backwards to build realistic funnel (each earlier stage > later stage)
            eng_75 = round(min(0.97, eng_100 + random.uniform(0.03, 0.08)), 4)
            eng_50 = round(min(0.98, eng_75 + random.uniform(0.05, 0.12)), 4)
            eng_25 = round(min(0.99, eng_50 + random.uniform(0.08, 0.15)), 4)
            eng_1 = round(min(1.0, eng_25 + random.uniform(0.02, 0.06)), 4)

            # Device distribution varies by region - use normalized percentages
            if video["region"] == "APAC":
                raw_desktop, raw_mobile, raw_tablet, raw_other = 0.38, 0.44, 0.12, 0.06
            elif video["region"] == "Americas":
                raw_desktop, raw_mobile, raw_tablet, raw_other = 0.55, 0.31, 0.11, 0.03
            else:  # EMEA
                raw_desktop, raw_mobile, raw_tablet, raw_other = 0.49, 0.35, 0.12, 0.04

            # Add small random variation while keeping sum = 1.0
            variation = random.uniform(-0.03, 0.03)
            desktop_pct = raw_desktop + variation
            mobile_pct = raw_mobile - variation  # Offset to maintain sum
            tablet_pct = raw_tablet + random.uniform(-0.02, 0.02)
            # Other absorbs any remainder
            other_pct = 1.0 - desktop_pct - mobile_pct - tablet_pct

            # Calculate views ensuring they sum exactly to daily_views
            views_desktop = int(daily_views * desktop_pct)
            views_mobile = int(daily_views * mobile_pct)
            views_tablet = int(daily_views * tablet_pct)
            views_other = max(0, daily_views - views_desktop - views_mobile - views_tablet)  # Remainder, never negative

            data.append({
                "video_id": video_id,
                "date": date,
                "year": date.year,
                "month": date.month,
                "quarter": f"Q{(date.month - 1) // 3 + 1}",
                "video_view": daily_views,
                "views_desktop": views_desktop,
                "views_mobile": views_mobile,
                "views_tablet": views_tablet,
                "views_other": views_other,
                "video_impression": int(daily_views * random.uniform(1.3, 2.2)),
                "play_rate": round(random.uniform(0.35, 0.75), 4),
                "engagement_score": round(random.uniform(0.35, 0.80), 4),
                "video_engagement_1": eng_1,
                "video_engagement_25": eng_25,
                "video_engagement_50": eng_50,
                "video_engagement_75": eng_75,
                "video_engagement_100": eng_100,
                "video_percent_viewed": round(eng_100 * 100 * random.uniform(0.9, 1.1), 2),
                "video_seconds_viewed": int(daily_views * video["video_duration_seconds"] * eng_50),
            })

    return pd.DataFrame(data)


# =============================================================================
# PRE-BUILT DEMO RESPONSES
# =============================================================================

DEMO_QUERIES = {
    "top_videos": {
        "patterns": ["top video", "top 10", "top perform", "best video", "most viewed", "popular", "highest views"],
        "sql": """SELECT
    d.name AS video_name,
    d.division_name AS division,
    d.region,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS completion_rate_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.name, d.division_name, d.region
ORDER BY total_views DESC
LIMIT 10""",
        "chart_type": "bar",
        "summary": "Your top performing video is '{top_video}' with {top_views:,} views."
    },

    "division_performance": {
        "patterns": ["compare division", "division performance", "by division", "division comparison", "which division", "ib performance", "gwm performance", "asset management", "group functions", "business unit"],
        "sql": """SELECT
    d.division_name AS division,
    COUNT(DISTINCT d.video_id) AS video_count,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS avg_completion_pct,
    ROUND(SUM(f.video_seconds_viewed) / 3600.0, 0) AS total_watch_hours
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.division_name
ORDER BY total_views DESC""",
        "chart_type": "bar",
        "summary": "Division performance comparison across the organization."
    },

    "regional_performance": {
        "patterns": ["regional performance", "regional comparison", "compare region", "by region", "apac performance", "emea performance", "americas performance", "europe", "asia pacific", "geographic"],
        "sql": """SELECT
    d.region,
    COUNT(DISTINCT d.video_id) AS video_count,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS avg_completion_pct,
    ROUND(SUM(views_mobile) * 100.0 / SUM(video_view), 1) AS mobile_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.region
ORDER BY total_views DESC""",
        "chart_type": "bar",
        "summary": "Regional performance shows distinct viewing patterns."
    },

    "quarterly_yoy": {
        "patterns": ["year over year", "yoy", "quarterly results", "compare quarter", "q1", "q2", "q3", "q4", "earnings"],
        "sql": """SELECT
    d.quarter,
    d.year,
    d.division_name,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS completion_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
WHERE d.video_content_type = 'Quarterly Results'
GROUP BY d.quarter, d.year, d.division_name
ORDER BY d.year, d.quarter""",
        "chart_type": "grouped_bar",
        "summary": "Year-over-year comparison of quarterly results content."
    },

    "funnel": {
        "patterns": ["funnel", "drop off", "dropoff", "retention", "engagement 1", "engagement 25", "engagement 50", "engagement 75", "engagement 100", "where do viewers"],
        "sql": """SELECT
    'Start (1%)' AS stage,
    ROUND(AVG(video_engagement_1) * 100, 1) AS retention_pct,
    1 AS stage_order
FROM facts
UNION ALL
SELECT '25%', ROUND(AVG(video_engagement_25) * 100, 1), 2 FROM facts
UNION ALL
SELECT '50%', ROUND(AVG(video_engagement_50) * 100, 1), 3 FROM facts
UNION ALL
SELECT '75%', ROUND(AVG(video_engagement_75) * 100, 1), 4 FROM facts
UNION ALL
SELECT 'Complete (100%)', ROUND(AVG(video_engagement_100) * 100, 1), 5 FROM facts
ORDER BY stage_order""",
        "chart_type": "funnel",
        "summary": "Viewer retention analysis shows the typical drop-off pattern."
    },

    "devices": {
        "patterns": ["device", "mobile vs desktop", "desktop vs mobile", "device breakdown", "mobile breakdown", "mobile usage", "mobile optimization", "tablet engagement", "tablet", "table", "platform breakdown", "how are people watching", "% is mobile", "% is tablet", "% is desktop"],
        "sql": """SELECT
    d.division_name AS division,
    d.region,
    ROUND(SUM(f.views_desktop) * 100.0 / SUM(f.video_view), 1) AS desktop_pct,
    ROUND(SUM(f.views_mobile) * 100.0 / SUM(f.video_view), 1) AS mobile_pct,
    ROUND(SUM(f.views_tablet) * 100.0 / SUM(f.video_view), 1) AS tablet_pct,
    ROUND(SUM(f.views_other) * 100.0 / SUM(f.video_view), 1) AS other_pct,
    SUM(f.video_view) AS total_views
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.division_name, d.region
ORDER BY d.division_name, d.region""",
        "chart_type": "stacked_bar",
        "summary": "Device usage varies significantly by division and region."
    },

    "channels": {
        "patterns": ["channel", "which channel", "department", "team performance"],
        "sql": """SELECT
    d.channel,
    d.division_name,
    COUNT(DISTINCT d.video_id) AS video_count,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS avg_completion_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.channel, d.division_name
ORDER BY total_views DESC""",
        "chart_type": "bar",
        "summary": "Channel performance across divisions."
    },

    "trends": {
        "patterns": ["trend", "over time", "monthly", "growth", "change", "how have views"],
        "sql": """SELECT
    DATE_TRUNC('month', f.date) AS month,
    d.division_name,
    SUM(f.video_view) AS total_views
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
WHERE f.date >= '2024-01-01'
GROUP BY DATE_TRUNC('month', f.date), d.division_name
ORDER BY month, d.division_name""",
        "chart_type": "line",
        "summary": "Viewing trends over time by division."
    },

    "content_types": {
        "patterns": ["content type", "what type", "category performance", "training vs", "marketing vs", "type performance"],
        "sql": """SELECT
    d.video_content_type AS content_type,
    COUNT(DISTINCT d.video_id) AS video_count,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(d.video_duration_seconds) / 60.0, 1) AS avg_duration_min,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS avg_completion_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.video_content_type
ORDER BY total_views DESC""",
        "chart_type": "bar",
        "summary": "Content type performance analysis."
    },

    "watch_time": {
        "patterns": ["watch time", "hours watched", "total time", "viewing time", "seconds viewed"],
        "sql": """SELECT
    d.division_name AS division,
    d.region,
    ROUND(SUM(f.video_seconds_viewed) / 3600.0, 1) AS total_watch_hours,
    SUM(f.video_view) AS total_views
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.division_name, d.region
ORDER BY total_watch_hours DESC""",
        "chart_type": "bar",
        "summary": "Watch time distribution across divisions and regions."
    },

    "training_compliance": {
        "patterns": ["training completion", "training video", "compliance video", "mandatory viewing", "required viewing", "compliance content"],
        "sql": """SELECT
    d.name AS video_name,
    d.division_name,
    d.region,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS completion_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
WHERE d.video_content_type IN ('Training', 'Compliance')
GROUP BY d.name, d.division_name, d.region
ORDER BY total_views DESC
LIMIT 10""",
        "chart_type": "bar",
        "summary": "Training and compliance content performance."
    },

    "correlation": {
        "patterns": ["correlation", "relationship", "correlate", "drives", "impact", "affect", "vs completion", "vs engagement"],
        "sql": """SELECT
    d.video_id,
    d.video_duration_seconds / 60.0 AS duration_min,
    SUM(f.video_view) AS total_views,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS completion_pct,
    d.video_content_type
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.video_id, d.video_duration_seconds, d.video_content_type
ORDER BY total_views DESC""",
        "chart_type": "scatter",
        "summary": "Correlation analysis between key metrics."
    },

    "chart_change": {
        "patterns": ["as a line", "as a bar", "as a pie", "show as", "change to", "horizontal bar", "scatter plot"],
        "sql": "-- Chart type change requested",
        "chart_type": "dynamic",
        "summary": "Changing visualization type."
    },

    "histogram": {
        "patterns": ["histogram", "distribution", "spread", "how are videos distributed", "completion distribution", "range of completion"],
        "sql": """SELECT
    d.video_id,
    d.name,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS completion_pct
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.video_id, d.name""",
        "chart_type": "histogram",
        "summary": "Distribution of video completion rates."
    },

    "outliers": {
        "patterns": ["outlier", "outliers", "anomaly", "anomalies", "extreme", "unusual", "problem videos", "underperforming", "overperforming", "quadrant"],
        "sql": """SELECT
    d.video_id,
    d.name,
    d.video_duration_seconds / 60.0 AS duration_min,
    ROUND(AVG(f.video_engagement_100) * 100, 1) AS completion_pct,
    d.video_content_type
FROM facts f
JOIN dimensions d ON f.video_id = d.video_id
GROUP BY d.video_id, d.name, d.video_duration_seconds, d.video_content_type""",
        "chart_type": "quadrant_scatter",
        "summary": "Identifying outlier videos by duration vs completion."
    },
}


def match_query(question: str) -> str:
    """Match user question to a demo query type."""
    question_lower = question.lower()

    # Check each query type for pattern matches
    for query_type, config in DEMO_QUERIES.items():
        for pattern in config["patterns"]:
            if pattern in question_lower:
                return query_type

    # Default to top videos
    return "top_videos"


def get_demo_response(query_type: str) -> dict:
    """Get demo response configuration."""
    return DEMO_QUERIES.get(query_type, DEMO_QUERIES["top_videos"])
