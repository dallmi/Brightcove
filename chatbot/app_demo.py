"""
Video Analytics Intelligence - Enhanced Demo

A showcase of natural language video analytics with rich visualizations.
Run: streamlit run app_demo.py
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

from config import APP_TITLE, APP_ICON, COLORS
from demo_data import (
    generate_demo_facts,
    generate_demo_dimensions,
    DEMO_QUERIES,
    match_query,
    get_demo_response,
)

# =============================================================================
# BENCHMARK DATA (Industry standards for context)
# =============================================================================

BENCHMARKS = {
    "completion_rate": {"industry": 38.0, "top_quartile": 52.0},
    "play_rate": {"industry": 45.0, "top_quartile": 62.0},
    "mobile_share": {"industry": 35.0, "trend": "increasing"},
    "avg_watch_time_min": {"industry": 4.2, "top_quartile": 6.8},
}

# =============================================================================
# PROACTIVE INSIGHTS GENERATION
# =============================================================================

def generate_proactive_insights(facts_df, dimensions_df):
    """Generate proactive insights and alerts for the landing page."""
    insights = []

    # 1. Check for underperforming recent content
    recent_videos = dimensions_df[dimensions_df["created_at"] >= datetime.now() - timedelta(days=90)]
    if len(recent_videos) > 0:
        recent_ids = recent_videos["video_id"].tolist()
        recent_facts = facts_df[facts_df["video_id"].isin(recent_ids)]
        if len(recent_facts) > 0:
            recent_completion = recent_facts["video_engagement_100"].mean() * 100
            overall_completion = facts_df["video_engagement_100"].mean() * 100
            if recent_completion < overall_completion * 0.85:
                drop_pct = ((overall_completion - recent_completion) / overall_completion * 100)
                insights.append({
                    "type": "warning",
                    "title": "Recent Content Underperforming",
                    "metric": f"-{drop_pct:.0f}%",
                    "detail": f"Videos from the last 90 days have {recent_completion:.1f}% completion vs {overall_completion:.1f}% overall.",
                    "action": "Review thumbnail and title strategy for recent uploads",
                    "priority": "High",
                    "owner": "Content Team"
                })

    # 2. Mobile opportunity detection
    mobile_views = facts_df["views_mobile"].sum()
    total_views = facts_df["video_view"].sum()
    mobile_pct = mobile_views / total_views * 100 if total_views > 0 else 0

    if mobile_pct > 35:
        # Check mobile completion vs desktop
        merged = facts_df.merge(dimensions_df[["video_id", "region"]], on="video_id")
        apac_data = merged[merged["region"] == "APAC"]
        if len(apac_data) > 0:
            apac_mobile_pct = apac_data["views_mobile"].sum() / apac_data["video_view"].sum() * 100
            if apac_mobile_pct > 40:
                insights.append({
                    "type": "opportunity",
                    "title": "APAC Mobile Optimization Opportunity",
                    "metric": f"{apac_mobile_pct:.0f}%",
                    "detail": f"APAC region has {apac_mobile_pct:.0f}% mobile viewership. Industry avg is {BENCHMARKS['mobile_share']['industry']}%.",
                    "action": "Add captions and create vertical cuts for top APAC content",
                    "priority": "Medium",
                    "owner": "Video Production",
                    "impact": "+15% mobile completion expected"
                })

    # 3. Training completion alert
    training_dims = dimensions_df[dimensions_df["video_content_type"].isin(["Training", "Compliance"])]
    if len(training_dims) > 0:
        training_facts = facts_df[facts_df["video_id"].isin(training_dims["video_id"])]
        if len(training_facts) > 0:
            training_completion = training_facts["video_engagement_100"].mean() * 100
            if training_completion < 70:
                insights.append({
                    "type": "alert",
                    "title": "Training Completion Below Target",
                    "metric": f"{training_completion:.0f}%",
                    "detail": f"Compliance training completion at {training_completion:.1f}% (target: 70%).",
                    "action": "Investigate drop-off points; consider shorter modules",
                    "priority": "High",
                    "owner": "L&D Team",
                    "impact": "Regulatory compliance risk"
                })
            else:
                insights.append({
                    "type": "success",
                    "title": "Training Completion Exceeds Target",
                    "metric": f"{training_completion:.0f}%",
                    "detail": f"Training completion at {training_completion:.1f}% - above 70% target.",
                    "action": "Document best practices for other content types",
                    "priority": "Low",
                    "owner": "L&D Team"
                })

    # 4. Quarterly results performance
    quarterly_dims = dimensions_df[dimensions_df["video_content_type"] == "Quarterly Results"]
    if len(quarterly_dims) > 0:
        quarterly_facts = facts_df[facts_df["video_id"].isin(quarterly_dims["video_id"])]
        if len(quarterly_facts) > 0:
            q_views = quarterly_facts["video_view"].sum()
            total = facts_df["video_view"].sum()
            q_share = q_views / total * 100 if total > 0 else 0
            insights.append({
                "type": "insight",
                "title": "Quarterly Results Drive Engagement",
                "metric": f"{q_share:.0f}%",
                "detail": f"Earnings content accounts for {q_share:.0f}% of total views with highest completion rates.",
                "action": "Apply earnings video format to other executive communications",
                "priority": "Medium",
                "owner": "Corp Comms"
            })

    # 5. Check for engagement drop-off patterns
    avg_start = facts_df["video_engagement_1"].mean() * 100
    avg_25 = facts_df["video_engagement_25"].mean() * 100
    early_dropoff = avg_start - avg_25

    if early_dropoff > 30:
        insights.append({
            "type": "warning",
            "title": "High Early Drop-off Detected",
            "metric": f"-{early_dropoff:.0f}%",
            "detail": f"Viewers drop from {avg_start:.0f}% to {avg_25:.0f}% in first quarter of videos.",
            "action": "Strengthen opening hooks; move key content to first 30 seconds",
            "priority": "High",
            "owner": "Video Production",
            "impact": "Recovering 10% could add 5,000+ completions/quarter"
        })

    return insights[:5]  # Return top 5 insights


def generate_recommended_actions(facts_df, dimensions_df):
    """Generate prioritized recommended actions."""
    actions = []

    # Calculate key metrics
    avg_completion = facts_df["video_engagement_100"].mean() * 100
    total_views = facts_df["video_view"].sum()

    # Action 1: Video optimization
    if avg_completion < BENCHMARKS["completion_rate"]["top_quartile"]:
        gap = BENCHMARKS["completion_rate"]["top_quartile"] - avg_completion
        potential_completions = int(total_views * gap / 100)
        actions.append({
            "title": "Optimize Video Length",
            "description": f"Current completion ({avg_completion:.1f}%) is {gap:.1f}pp below top quartile. Shortening videos under 5 min could recover {potential_completions:,} completions.",
            "impact": "High",
            "effort": "Medium",
            "owner": "Video Production",
            "deadline": "Q2 2025"
        })

    # Action 2: Mobile optimization
    mobile_share = facts_df["views_mobile"].sum() / total_views * 100
    if mobile_share > 30:
        actions.append({
            "title": "Launch Mobile-First Initiative",
            "description": f"With {mobile_share:.0f}% mobile viewers, prioritize: captions, vertical formats, chapter markers.",
            "impact": "High",
            "effort": "Low",
            "owner": "Content Team",
            "deadline": "Next sprint"
        })

    # Action 3: Replicate success patterns
    merged = facts_df.merge(dimensions_df[["video_id", "video_content_type"]], on="video_id")
    type_completion = merged.groupby("video_content_type")["video_engagement_100"].mean() * 100
    best_type = type_completion.idxmax()
    best_rate = type_completion.max()
    actions.append({
        "title": f"Replicate {best_type} Success",
        "description": f"{best_type} content achieves {best_rate:.1f}% completion. Apply format patterns to underperforming categories.",
        "impact": "Medium",
        "effort": "Low",
        "owner": "Content Strategy",
        "deadline": "Ongoing"
    })

    # Action 4: Regional targeting
    actions.append({
        "title": "Implement Regional Content Strategy",
        "description": "APAC, EMEA, and Americas show distinct viewing patterns. Create region-specific distribution schedules.",
        "impact": "Medium",
        "effort": "Medium",
        "owner": "Distribution Team",
        "deadline": "Q2 2025"
    })

    return actions

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# CUSTOM CSS - Clean Corporate Design (minimal red)
# =============================================================================

st.markdown(f"""
<style>
    /* Import professional font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global styles */
    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        color: {COLORS['dark']};
    }}

    /* Main container */
    .main .block-container {{
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }}

    /* Header styling - clean, minimal */
    .main-header {{
        border-bottom: 3px solid {COLORS['accent']};
        padding: 1rem 0 1.5rem 0;
        margin-bottom: 1.5rem;
    }}

    .main-header h1 {{
        margin: 0;
        font-size: 1.75rem;
        font-weight: 700;
        color: {COLORS['black']};
        letter-spacing: -0.03em;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}

    .main-header p {{
        margin: 0.5rem 0 0 0;
        color: {COLORS['gray']};
        font-size: 0.95rem;
    }}

    .accent-diamond {{
        color: {COLORS['accent']};
        font-size: 1.5rem;
    }}

    /* Demo mode indicator - subtle */
    .demo-indicator {{
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        padding: 0.35rem 0.75rem;
        border-radius: 4px;
        font-size: 0.75rem;
        color: {COLORS['gray']};
        margin-left: 1rem;
    }}

    .demo-dot {{
        width: 6px;
        height: 6px;
        background: {COLORS['success']};
        border-radius: 50%;
    }}

    /* Card styling */
    .card {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }}

    .card-header {{
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: {COLORS['gray']};
        margin-bottom: 0.75rem;
    }}

    /* User message */
    .user-message {{
        background: {COLORS['surface']};
        border-left: 3px solid {COLORS['black']};
        padding: 1rem 1.25rem;
        margin: 1rem 0;
        border-radius: 0 6px 6px 0;
    }}

    .user-message strong {{
        color: {COLORS['black']};
        font-weight: 600;
    }}

    /* Assistant response */
    .assistant-response {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1.5rem;
        margin: 1rem 0;
    }}

    /* SQL display */
    .sql-container {{
        background: #1a1a2e;
        border-radius: 6px;
        padding: 1rem;
        margin: 1rem 0;
        overflow-x: auto;
    }}

    .sql-container code {{
        color: #a8dadc;
        font-family: 'SF Mono', 'Monaco', 'Inconsolata', monospace;
        font-size: 0.8rem;
        line-height: 1.5;
    }}

    /* Metric cards */
    .metric-row {{
        display: flex;
        gap: 1rem;
        margin: 1rem 0;
    }}

    .metric-card {{
        flex: 1;
        background: {COLORS['surface']};
        border-radius: 6px;
        padding: 1rem;
        text-align: center;
    }}

    .metric-value {{
        font-size: 1.5rem;
        font-weight: 700;
        color: {COLORS['black']};
        line-height: 1.2;
    }}

    .metric-label {{
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: {COLORS['gray']};
        margin-top: 0.25rem;
    }}

    .metric-change {{
        font-size: 0.75rem;
        margin-top: 0.25rem;
    }}

    .metric-up {{
        color: {COLORS['success']};
    }}

    .metric-down {{
        color: {COLORS['error']};
    }}

    /* Insight box */
    .insight-box {{
        background: linear-gradient(135deg, {COLORS['surface']} 0%, #fff 100%);
        border-left: 3px solid {COLORS['chart_1']};
        padding: 1rem 1.25rem;
        border-radius: 0 6px 6px 0;
        margin: 1rem 0;
    }}

    .insight-box h4 {{
        margin: 0 0 0.5rem 0;
        font-size: 0.8rem;
        font-weight: 600;
        color: {COLORS['chart_1']};
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }}

    .insight-box p {{
        margin: 0;
        color: {COLORS['dark']};
        font-size: 0.9rem;
        line-height: 1.5;
    }}

    /* Suggestion chips - ChatGPT/Copilot subtle style */
    .suggestions-container {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        padding: 0.5rem 0;
        justify-content: center;
        opacity: 0;
        max-height: 0;
        overflow: hidden;
        transition: opacity 0.2s ease, max-height 0.2s ease, padding 0.2s ease;
    }}

    .suggestions-container.visible {{
        opacity: 1;
        max-height: 80px;
        padding: 0.75rem 0 0.5rem 0;
    }}

    .suggestions-label {{
        font-size: 0.7rem;
        letter-spacing: 0.02em;
        color: {COLORS['gray_light']};
        margin-bottom: 0.5rem;
        text-align: center;
        width: 100%;
    }}

    /* Minimal suggestion buttons - no borders, just text with sparkle */
    .suggestion-btn-container {{
        margin-bottom: 0.5rem;
    }}

    .suggestion-btn-container .stButton > button {{
        background: transparent !important;
        border: none !important;
        padding: 0.35rem 0.6rem !important;
        border-radius: 4px !important;
        font-size: 0.8rem !important;
        font-weight: 400 !important;
        color: {COLORS['gray']} !important;
        cursor: pointer !important;
        transition: all 0.15s ease !important;
        white-space: nowrap !important;
        box-shadow: none !important;
    }}

    .suggestion-btn-container .stButton > button:hover {{
        color: {COLORS['dark']} !important;
        background: {COLORS['surface']} !important;
    }}

    .suggestion-btn-container .stButton > button:active,
    .suggestion-btn-container .stButton > button:focus {{
        background: {COLORS['border']} !important;
        box-shadow: none !important;
        outline: none !important;
    }}

    /* Sidebar styling */
    [data-testid="stSidebar"] {{
        background: {COLORS['background']};
        border-right: 1px solid {COLORS['border']};
    }}

    [data-testid="stSidebar"] .block-container {{
        padding-top: 1.5rem;
    }}

    .sidebar-section {{
        margin-bottom: 1.5rem;
    }}

    .sidebar-title {{
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: {COLORS['gray']};
        margin-bottom: 0.75rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid {COLORS['border']};
    }}

    /* Button styling - for sidebar and action buttons only */
    [data-testid="stSidebar"] .stButton > button {{
        background: {COLORS['black']};
        color: white;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        font-size: 0.85rem;
        transition: all 0.15s ease;
    }}

    [data-testid="stSidebar"] .stButton > button:hover {{
        background: {COLORS['dark']};
    }}

    /* Primary action button (accent) */
    .primary-btn > button {{
        background: {COLORS['accent']} !important;
    }}

    .primary-btn > button:hover {{
        background: #cc0000 !important;
    }}

    /* Input styling */
    .stTextInput > div > div > input {{
        border-radius: 6px;
        border: 1px solid {COLORS['border']};
        padding: 0.75rem 1rem;
        font-size: 0.95rem;
    }}

    .stTextInput > div > div > input:focus {{
        border-color: {COLORS['black']};
        box-shadow: 0 0 0 1px {COLORS['black']};
    }}

    /* Table styling */
    .dataframe {{
        font-size: 0.85rem !important;
        border: none !important;
    }}

    .dataframe th {{
        background: {COLORS['surface']} !important;
        color: {COLORS['dark']} !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.7rem !important;
        letter-spacing: 0.05em;
        padding: 0.75rem !important;
        border-bottom: 2px solid {COLORS['border']} !important;
    }}

    .dataframe td {{
        padding: 0.75rem !important;
        border-bottom: 1px solid {COLORS['border']} !important;
    }}

    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}

    /* Expander styling */
    .streamlit-expanderHeader {{
        font-size: 0.85rem;
        font-weight: 500;
        color: {COLORS['dark']};
    }}

    /* Status pills */
    .status-pill {{
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.25rem 0.6rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 500;
    }}

    .status-connected {{
        background: #E8F5E9;
        color: {COLORS['success']};
    }}

    .status-demo {{
        background: {COLORS['surface']};
        color: {COLORS['gray']};
    }}

    /* Proactive Insight Cards */
    .insight-card {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.75rem;
        position: relative;
        overflow: hidden;
    }}

    .insight-card::before {{
        content: '';
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        width: 4px;
    }}

    .insight-card.warning::before {{
        background: {COLORS['warning']};
    }}

    .insight-card.alert::before {{
        background: {COLORS['error']};
    }}

    .insight-card.opportunity::before {{
        background: {COLORS['chart_1']};
    }}

    .insight-card.success::before {{
        background: {COLORS['success']};
    }}

    .insight-card.insight::before {{
        background: {COLORS['chart_3']};
    }}

    .insight-card-header {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 0.5rem;
    }}

    .insight-card-title {{
        font-size: 0.9rem;
        font-weight: 600;
        color: {COLORS['black']};
        margin: 0;
    }}

    .insight-card-metric {{
        font-size: 1.1rem;
        font-weight: 700;
        padding: 0.2rem 0.5rem;
        border-radius: 4px;
        background: {COLORS['surface']};
    }}

    .insight-card-metric.negative {{
        color: {COLORS['error']};
        background: #FFF5F5;
    }}

    .insight-card-metric.positive {{
        color: {COLORS['success']};
        background: #E8F5E9;
    }}

    .insight-card-detail {{
        font-size: 0.85rem;
        color: {COLORS['gray_dark']};
        margin-bottom: 0.75rem;
        line-height: 1.4;
    }}

    .insight-card-action {{
        background: {COLORS['surface']};
        padding: 0.6rem 0.8rem;
        border-radius: 6px;
        font-size: 0.8rem;
    }}

    .insight-card-action strong {{
        color: {COLORS['black']};
    }}

    .insight-card-meta {{
        display: flex;
        gap: 1rem;
        margin-top: 0.5rem;
        font-size: 0.7rem;
        color: {COLORS['gray']};
    }}

    .insight-card-meta span {{
        display: flex;
        align-items: center;
        gap: 0.25rem;
    }}

    /* Action Items */
    .action-item {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 6px;
        padding: 0.75rem;
        margin-bottom: 0.5rem;
    }}

    .action-item-title {{
        font-size: 0.85rem;
        font-weight: 600;
        color: {COLORS['black']};
        margin-bottom: 0.25rem;
    }}

    .action-item-desc {{
        font-size: 0.75rem;
        color: {COLORS['gray_dark']};
        line-height: 1.4;
    }}

    .action-item-badges {{
        display: flex;
        gap: 0.5rem;
        margin-top: 0.5rem;
        flex-wrap: wrap;
    }}

    .action-badge {{
        font-size: 0.65rem;
        padding: 0.15rem 0.4rem;
        border-radius: 3px;
        font-weight: 500;
    }}

    .action-badge.impact-high {{
        background: #E8F5E9;
        color: {COLORS['success']};
    }}

    .action-badge.impact-medium {{
        background: #FFF8E1;
        color: {COLORS['warning']};
    }}

    .action-badge.effort-low {{
        background: #E3F2FD;
        color: {COLORS['chart_1']};
    }}

    .action-badge.effort-medium {{
        background: #F3E5F5;
        color: {COLORS['chart_3']};
    }}

    /* Enhanced Insight Box with Actionable Format */
    .actionable-insight {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1.25rem;
        margin: 1rem 0;
    }}

    .actionable-insight-header {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 0.75rem;
        padding-bottom: 0.75rem;
        border-bottom: 1px solid {COLORS['border']};
    }}

    .actionable-insight h4 {{
        margin: 0;
        font-size: 0.9rem;
        font-weight: 600;
        color: {COLORS['chart_1']};
    }}

    .benchmark-tag {{
        font-size: 0.7rem;
        padding: 0.2rem 0.5rem;
        background: {COLORS['surface']};
        border-radius: 4px;
        color: {COLORS['gray']};
    }}

    .actionable-insight-body {{
        font-size: 0.9rem;
        color: {COLORS['dark']};
        line-height: 1.6;
    }}

    .actionable-insight-actions {{
        margin-top: 1rem;
        padding-top: 1rem;
        border-top: 1px solid {COLORS['border']};
    }}

    .actionable-insight-actions h5 {{
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: {COLORS['gray']};
        margin: 0 0 0.5rem 0;
    }}

    .action-step {{
        display: flex;
        align-items: flex-start;
        gap: 0.5rem;
        padding: 0.4rem 0;
        font-size: 0.85rem;
    }}

    .action-step-num {{
        background: {COLORS['chart_1']};
        color: white;
        width: 18px;
        height: 18px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.7rem;
        font-weight: 600;
        flex-shrink: 0;
    }}

    .insight-footer {{
        display: flex;
        justify-content: space-between;
        margin-top: 1rem;
        padding-top: 0.75rem;
        border-top: 1px solid {COLORS['border']};
        font-size: 0.75rem;
        color: {COLORS['gray']};
    }}

    .insight-footer-item {{
        display: flex;
        align-items: center;
        gap: 0.25rem;
    }}

    /* Export Button */
    .export-btn {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 6px;
        padding: 0.5rem 1rem;
        font-size: 0.8rem;
        color: {COLORS['gray_dark']};
        cursor: pointer;
        transition: all 0.15s ease;
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
    }}

    .export-btn:hover {{
        background: {COLORS['background']};
        border-color: {COLORS['black']};
        color: {COLORS['black']};
    }}

    /* Data Freshness Indicator */
    .data-freshness {{
        font-size: 0.7rem;
        color: {COLORS['gray_light']};
        display: flex;
        align-items: center;
        gap: 0.25rem;
    }}

    .freshness-dot {{
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: {COLORS['success']};
    }}

    /* Section headers */
    .section-header {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
    }}

    .section-title {{
        font-size: 1rem;
        font-weight: 600;
        color: {COLORS['black']};
        margin: 0;
    }}

    /* Priority badges */
    .priority-high {{
        color: {COLORS['error']};
    }}

    .priority-medium {{
        color: {COLORS['warning']};
    }}

    .priority-low {{
        color: {COLORS['success']};
    }}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# SESSION STATE
# =============================================================================

if "demo_messages" not in st.session_state:
    st.session_state.demo_messages = []

if "demo_dimensions" not in st.session_state:
    st.session_state.demo_dimensions = generate_demo_dimensions(80)

if "demo_facts" not in st.session_state:
    st.session_state.demo_facts = generate_demo_facts(st.session_state.demo_dimensions)

if "selected_suggestion" not in st.session_state:
    st.session_state.selected_suggestion = None

# Store last query result for chart type switching
if "last_result" not in st.session_state:
    st.session_state.last_result = None

# Generate proactive insights once
if "proactive_insights" not in st.session_state:
    st.session_state.proactive_insights = None

if "recommended_actions" not in st.session_state:
    st.session_state.recommended_actions = None

# Data freshness timestamp
if "data_timestamp" not in st.session_state:
    st.session_state.data_timestamp = datetime.now()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_bar_chart(df, x_col, y_col, title="", color=COLORS['chart_1']):
    """Create a clean bar chart."""
    fig = px.bar(
        df, x=x_col, y=y_col,
        color_discrete_sequence=[color]
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        title="" if not title else dict(text=title, font_size=14, font_weight=600),
        margin=dict(l=20, r=20, t=40 if title else 20, b=20),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor=COLORS['border'],
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLORS['border'],
            showline=False,
        ),
        showlegend=False,
    )
    return fig


def create_funnel_chart(df, base_views=10000):
    """Create engagement funnel visualization with actual view counts."""
    # Calculate actual views at each stage
    views_at_stage = (df['retention_pct'] / 100 * base_views).round(0).astype(int)

    # Create custom text labels showing both views and retention percentage
    text_labels = [
        f"{views:,}<br>({pct:.1f}%)"
        for views, pct in zip(views_at_stage, df['retention_pct'])
    ]

    # Ensure we have colors for all stages
    num_stages = len(df)
    colors = [COLORS['chart_1'], COLORS['chart_2'], COLORS['chart_3'],
              COLORS['chart_4'], COLORS['chart_5']][:num_stages]

    fig = go.Figure(go.Funnel(
        y=df['stage'].tolist(),
        x=views_at_stage.tolist(),
        text=text_labels,
        textinfo="text",
        marker=dict(color=colors),
        connector=dict(line=dict(color=COLORS['border'], width=2))
    ))
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        height=400,
        margin=dict(l=120, r=20, t=50, b=20),
        showlegend=False,
        title=dict(
            text=f"Viewer Journey (base: {base_views:,} views)",
            font=dict(size=14, color=COLORS['gray']),
            x=0.5,
            xanchor='center'
        ),
        funnelmode="stack",
        yaxis=dict(
            tickfont=dict(size=12),
            automargin=True
        )
    )
    return fig


def create_funnel_bar_chart(df, base_views=10000):
    """Create funnel as horizontal bar chart alternative."""
    views_at_stage = (df['retention_pct'] / 100 * base_views).round(0).astype(int)
    df_plot = df.copy()
    df_plot['views'] = views_at_stage
    df_plot['drop_off'] = base_views - views_at_stage

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_plot['stage'],
        x=df_plot['views'],
        orientation='h',
        name='Retained',
        marker_color=COLORS['chart_1'],
        text=df_plot['views'].apply(lambda x: f'{x:,}'),
        textposition='inside',
    ))
    fig.add_trace(go.Bar(
        y=df_plot['stage'],
        x=df_plot['drop_off'],
        orientation='h',
        name='Drop-off',
        marker_color=COLORS['border'],
        text=df_plot['drop_off'].apply(lambda x: f'-{x:,}'),
        textposition='inside',
    ))
    fig.update_layout(
        barmode='stack',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        margin=dict(l=100, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        title=dict(text="Viewer Retention vs Drop-off", font=dict(size=14, color=COLORS['gray'])),
    )
    return fig


def create_pie_chart(df, values_col, names_col):
    """Create a clean pie/donut chart."""
    fig = px.pie(
        df, values=values_col, names=names_col,
        color_discrete_sequence=[COLORS['chart_1'], COLORS['chart_2'],
                                  COLORS['chart_3'], COLORS['chart_4']],
        hole=0.4
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2)
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig


def create_line_chart(df, x_col, y_col, title=""):
    """Create a trend line chart."""
    fig = px.line(
        df, x=x_col, y=y_col,
        color_discrete_sequence=[COLORS['chart_1']],
        markers=True
    )
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        title="" if not title else dict(text=title, font_size=14, font_weight=600),
        margin=dict(l=20, r=20, t=40 if title else 20, b=20),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor=COLORS['border'],
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLORS['border'],
            showline=False,
        ),
        showlegend=False,
    )
    fig.update_traces(line=dict(width=2), marker=dict(size=6))
    return fig


def create_scatter_chart(df, x_col, y_col, size_col=None, color_col=None, title=""):
    """Create a scatter plot for correlation analysis with optional regression line."""
    # Try to use trendline if statsmodels is available
    try:
        fig = px.scatter(
            df, x=x_col, y=y_col,
            size=size_col if size_col else None,
            color=color_col if color_col else None,
            color_discrete_sequence=[
                COLORS['chart_1'], COLORS['chart_2'], COLORS['chart_3'],
                COLORS['chart_4'], COLORS['chart_5'], COLORS.get('chart_6', '#9CA3AF'),
                COLORS.get('chart_7', '#6366F1'), COLORS.get('chart_8', '#EC4899')
            ],
            hover_data=df.columns.tolist(),
            trendline="ols",  # Single OLS regression line across all data
            trendline_scope="overall",  # One line for ALL points, not per-category
            trendline_color_override=COLORS['accent'],  # Consistent trend line color
        )
        # Style the trendline to be prominent but not overwhelming
        for trace in fig.data:
            if trace.mode == "lines":
                trace.update(
                    line=dict(width=3, dash='solid'),
                    name='Trend (Linear Regression)',
                    showlegend=True
                )
    except (ImportError, ModuleNotFoundError):
        # statsmodels not installed, create scatter without trendline
        fig = px.scatter(
            df, x=x_col, y=y_col,
            size=size_col if size_col else None,
            color=color_col if color_col else None,
            color_discrete_sequence=[
                COLORS['chart_1'], COLORS['chart_2'], COLORS['chart_3'],
                COLORS['chart_4'], COLORS['chart_5'], COLORS.get('chart_6', '#9CA3AF'),
                COLORS.get('chart_7', '#6366F1'), COLORS.get('chart_8', '#EC4899')
            ],
            hover_data=df.columns.tolist(),
        )

    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        title="" if not title else dict(text=title, font=dict(size=16, color=COLORS['dark'])),
        margin=dict(l=60, r=140, t=60 if title else 30, b=60),
        xaxis=dict(
            showgrid=True,
            gridcolor=COLORS['border'],
            title=dict(text=x_col, font=dict(size=13, color=COLORS['dark'])),
            zeroline=False,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=COLORS['border'],
            title=dict(text=y_col, font=dict(size=13, color=COLORS['dark'])),
            zeroline=False,
        ),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.02,
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor=COLORS['border'],
            borderwidth=1,
            font=dict(size=11),
        ),
    )
    return fig


def create_stacked_bar_chart(df, x_col, y_cols, title=""):
    """Create a stacked bar chart."""
    fig = go.Figure()

    colors = [COLORS['chart_1'], COLORS['chart_2'], COLORS['chart_3'], COLORS['chart_4']]

    for i, col in enumerate(y_cols):
        fig.add_trace(go.Bar(
            x=df[x_col],
            y=df[col],
            name=col,
            marker_color=colors[i % len(colors)],
            text=df[col].apply(lambda x: f'{x:.1f}%'),
            textposition='inside',
        ))

    fig.update_layout(
        barmode='stack',
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        title="" if not title else dict(text=title, font_size=14, color=COLORS['gray']),
        margin=dict(l=40, r=20, t=60 if title else 20, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor=COLORS['border'], title="Percentage"),
    )
    return fig


def create_histogram_chart(df, value_col, title="", bins=20, benchmark=None):
    """Create a histogram showing distribution of values with optional benchmark line."""
    fig = go.Figure()

    # Create histogram
    fig.add_trace(go.Histogram(
        x=df[value_col],
        nbinsx=bins,
        marker_color=COLORS['chart_1'],
        marker_line_color=COLORS['dark'],
        marker_line_width=1,
        opacity=0.85,
        name=value_col,
        hovertemplate=f'{value_col}: %{{x:.1f}}<br>Count: %{{y}}<extra></extra>'
    ))

    # Add benchmark line if provided
    if benchmark is not None:
        fig.add_vline(
            x=benchmark,
            line_dash="dash",
            line_color=COLORS['accent'],
            line_width=2,
            annotation_text=f"Industry Avg: {benchmark}%",
            annotation_position="top",
            annotation_font_color=COLORS['accent']
        )

    # Add mean line
    mean_val = df[value_col].mean()
    fig.add_vline(
        x=mean_val,
        line_dash="solid",
        line_color=COLORS['chart_3'],
        line_width=2,
        annotation_text=f"Your Avg: {mean_val:.1f}%",
        annotation_position="top right",
        annotation_font_color=COLORS['chart_3']
    )

    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        title=dict(text=title, font=dict(size=14, color=COLORS['gray']), x=0.5, xanchor='center') if title else None,
        margin=dict(l=60, r=40, t=60 if title else 30, b=60),
        xaxis=dict(
            title=dict(text=value_col, font=dict(size=12)),
            showgrid=True,
            gridcolor=COLORS['border'],
        ),
        yaxis=dict(
            title=dict(text="Number of Videos", font=dict(size=12)),
            showgrid=True,
            gridcolor=COLORS['border'],
        ),
        bargap=0.05,
        showlegend=False,
    )
    return fig


def create_quadrant_scatter_chart(df, x_col, y_col, label_col=None, x_threshold=None, y_threshold=None, title=""):
    """Create a scatter plot with quadrant lines for outlier identification."""
    # Calculate thresholds if not provided (use medians)
    if x_threshold is None:
        x_threshold = df[x_col].median()
    if y_threshold is None:
        y_threshold = df[y_col].median()

    # Determine quadrant for each point
    def get_quadrant(row):
        if row[x_col] >= x_threshold and row[y_col] >= y_threshold:
            return "High Duration + High Completion"
        elif row[x_col] < x_threshold and row[y_col] >= y_threshold:
            return "Short + High Completion ✓"
        elif row[x_col] >= x_threshold and row[y_col] < y_threshold:
            return "Long + Low Completion ⚠"
        else:
            return "Short + Low Completion"

    df_plot = df.copy()
    df_plot['Quadrant'] = df_plot.apply(get_quadrant, axis=1)

    # Color mapping for quadrants
    quadrant_colors = {
        "High Duration + High Completion": COLORS['chart_1'],
        "Short + High Completion ✓": COLORS['chart_2'],
        "Long + Low Completion ⚠": COLORS['chart_4'],
        "Short + Low Completion": COLORS['chart_3'],
    }

    fig = go.Figure()

    # Add scatter points by quadrant
    for quadrant, color in quadrant_colors.items():
        mask = df_plot['Quadrant'] == quadrant
        quadrant_df = df_plot[mask]
        if len(quadrant_df) > 0:
            hover_text = quadrant_df[label_col] if label_col and label_col in df_plot.columns else None
            fig.add_trace(go.Scatter(
                x=quadrant_df[x_col],
                y=quadrant_df[y_col],
                mode='markers',
                name=f"{quadrant} ({len(quadrant_df)})",
                marker=dict(
                    color=color,
                    size=10,
                    line=dict(width=1, color='white'),
                    opacity=0.8
                ),
                text=hover_text,
                hovertemplate=f"<b>%{{text}}</b><br>{x_col}: %{{x:.1f}}<br>{y_col}: %{{y:.1f}}%<extra>{quadrant}</extra>" if hover_text is not None else f"{x_col}: %{{x:.1f}}<br>{y_col}: %{{y:.1f}}%<extra>{quadrant}</extra>",
            ))

    # Add threshold lines
    fig.add_hline(
        y=y_threshold,
        line_dash="dash",
        line_color=COLORS['gray'],
        line_width=1.5,
        annotation_text=f"Completion threshold: {y_threshold:.0f}%",
        annotation_position="right",
        annotation_font_size=10,
        annotation_font_color=COLORS['gray']
    )
    fig.add_vline(
        x=x_threshold,
        line_dash="dash",
        line_color=COLORS['gray'],
        line_width=1.5,
        annotation_text=f"Duration threshold: {x_threshold:.1f} min",
        annotation_position="top",
        annotation_font_size=10,
        annotation_font_color=COLORS['gray']
    )

    # Add quadrant labels in corners
    x_min, x_max = df[x_col].min(), df[x_col].max()
    y_min, y_max = df[y_col].min(), df[y_col].max()
    x_pad = (x_max - x_min) * 0.05
    y_pad = (y_max - y_min) * 0.05

    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font_family="Inter",
        font_color=COLORS['dark'],
        title=dict(text=title, font=dict(size=14, color=COLORS['gray']), x=0.5, xanchor='center') if title else None,
        height=500,
        margin=dict(l=60, r=40, t=60 if title else 30, b=60),
        xaxis=dict(
            title=dict(text=x_col, font=dict(size=12)),
            showgrid=True,
            gridcolor=COLORS['border'],
            zeroline=False,
        ),
        yaxis=dict(
            title=dict(text=y_col, font=dict(size=12)),
            showgrid=True,
            gridcolor=COLORS['border'],
            zeroline=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=10),
        ),
    )
    return fig


def parse_chart_type_request(query: str) -> str:
    """Parse the desired chart type from a chart change request."""
    query_lower = query.lower()

    if "pie" in query_lower:
        return "pie"
    elif "line" in query_lower:
        return "line"
    elif "scatter" in query_lower:
        return "scatter"
    elif "horizontal bar" in query_lower:
        return "horizontal_bar"
    elif "bar" in query_lower:
        return "bar"
    elif "funnel" in query_lower:
        return "funnel"
    elif "stacked" in query_lower:
        return "stacked_bar"

    return "bar"  # Default


def get_contextual_suggestions(last_query_type=None, last_chart_type=None, last_data=None):
    """Get smart suggestions based on context, with actionable follow-ups."""
    base_suggestions = [
        "Top performing videos",
        "Compare divisions",
        "Engagement funnel",
        "Mobile vs desktop breakdown",
    ]

    # Actionable follow-up suggestions based on last query
    if last_query_type == "top_videos":
        return [
            "Compare divisions",
            "Show engagement funnel",
            "Regional comparison",
            "Show as pie chart",
        ]

    elif last_query_type == "division_performance":
        return [
            "Which division improved most?",
            "Regional comparison",
            "Training video completion",
            "Show as pie chart",
        ]

    elif last_query_type == "regional_performance":
        return [
            "Device breakdown",
            "Mobile vs desktop breakdown",
            "Content type performance",
            "Show as pie chart",
        ]

    elif last_query_type == "quarterly_yoy":
        return [
            "Why did Q4 outperform?",
            "Compare divisions",
            "Show trends over time",
            "Show as line chart",
        ]

    elif last_query_type == "funnel":
        return [
            "Top performing videos",
            "Content type performance",
            "Duration vs completion correlation",
            "Show as bar chart",
        ]

    elif last_query_type == "devices":
        return [
            "Mobile optimization opportunities",
            "Regional comparison",
            "Tablet engagement analysis",
            "Show as pie chart",
        ]

    elif last_query_type == "correlation":
        return [
            "Optimal video duration",
            "Best content type analysis",
            "Completion by division",
            "Show as line chart",
        ]

    elif last_query_type == "training_compliance":
        return [
            "Which training needs attention?",
            "Compliance by region",
            "Apply training patterns to other content",
            "Show completion funnel",
        ]

    elif last_query_type == "trends":
        return [
            "What caused the peaks?",
            "Seasonal patterns analysis",
            "Content calendar optimization",
            "Show quarterly YoY",
        ]

    elif last_query_type == "histogram":
        return [
            "Show outlier analysis",
            "Top performing videos",
            "Content type breakdown",
            "Show as bar chart",
        ]

    elif last_query_type == "outliers":
        return [
            "Show completion distribution",
            "Top performing videos",
            "Division performance",
            "Show as scatter plot",
        ]

    return base_suggestions


def generate_demo_result(query_type, facts_df, dimensions_df):
    """Generate demo result data based on query type."""
    config = get_demo_response(query_type)

    if query_type == "top_videos":
        merged = dimensions_df.merge(
            facts_df.groupby("video_id").agg({
                "video_view": "sum",
                "engagement_score": "mean",
                "video_engagement_100": "mean"
            }).reset_index(),
            on="video_id"
        )
        df = merged.nlargest(10, "video_view")[[
            "name", "division_name", "region", "video_view",
            "video_engagement_100"
        ]].copy()
        df.columns = ["Video", "Division", "Region", "Views", "Completion"]
        df["Completion"] = (df["Completion"] * 100).round(1).astype(str) + "%"
        # Keep Views as numeric for charting, format for display
        df["Views_Display"] = df["Views"].apply(lambda x: f"{x:,}")

        top_video = df.iloc[0]['Video']
        top_views = df.iloc[0]['Views']
        top_division = df['Division'].mode().iloc[0]
        top_10_total = df['Views'].sum()

        summary = f"""**Top Performer:** {top_video} leads with {top_views:,} views.

**Pattern Identified:** {top_division} dominates the top 10, accounting for {df[df['Division']==top_division]['Views'].sum()/top_10_total*100:.0f}% of top content views.

**Recommended Actions:**
1. **Replicate success patterns** from "{top_video[:30]}..." format to other divisions
2. **Promote underrepresented regions** - consider targeted distribution for lower-performing areas
3. **Analyze timing** - review publish dates of top videos to optimize release schedule

**Owner:** Content Strategy | **Impact:** Applying top patterns could lift avg views by 15-20%"""

        # Use Views for chart, drop display column
        df_chart = df.drop(columns=["Views_Display"])
        return df_chart, config["chart_type"], summary

    elif query_type == "division_performance":
        merged = facts_df.merge(dimensions_df[["video_id", "division_name"]], on="video_id")
        div_stats = merged.groupby("division_name").agg({
            "video_view": "sum",
            "video_id": "nunique",
            "video_engagement_100": "mean",
            "video_seconds_viewed": "sum"
        }).reset_index()
        div_stats.columns = ["Division", "Views", "Videos", "Completion", "Watch Time"]
        div_stats["Watch Hours"] = (div_stats["Watch Time"] / 3600).round(0).astype(int)
        div_stats["Completion_Pct"] = div_stats["Completion"] * 100
        div_stats["Completion"] = div_stats["Completion_Pct"].round(1).astype(str) + "%"
        div_stats = div_stats.sort_values("Views", ascending=False)
        df = div_stats[["Division", "Videos", "Views", "Completion", "Watch Hours"]]

        top_div = df.iloc[0]["Division"]
        bottom_div = df.iloc[-1]["Division"]
        top_views = df.iloc[0]["Views"]
        bottom_views = df.iloc[-1]["Views"]
        gap = top_views - bottom_views

        # Find highest completion
        best_completion_div = div_stats.loc[div_stats["Completion_Pct"].idxmax(), "Division"]
        best_completion = div_stats["Completion_Pct"].max()

        summary = f"""**Performance Gap Identified:** {gap:,} view difference between {top_div} ({top_views:,}) and {bottom_div} ({bottom_views:,}).

**Best Practice:** {best_completion_div} achieves {best_completion:.1f}% completion rate (benchmark: {BENCHMARKS['completion_rate']['industry']}%).

**Recommended Actions:**
1. **Cross-pollinate content:** Share {best_completion_div}'s engagement tactics with {bottom_div}
2. **Resource reallocation:** Consider shifting 20% of {bottom_div} production budget to proven formats
3. **Executive briefing:** Schedule review with {bottom_div} leadership on content strategy

**Owner:** Division Heads | **Timeline:** Q2 planning cycle | **Impact:** Closing gap could add {gap//2:,}+ views"""

        return df, "bar", summary

    elif query_type == "regional_performance":
        merged = facts_df.merge(dimensions_df[["video_id", "region"]], on="video_id")
        region_stats = merged.groupby("region").agg({
            "video_view": "sum",
            "video_id": "nunique",
            "video_engagement_100": "mean",
            "views_mobile": "sum"
        }).reset_index()
        total_views = region_stats["video_view"].sum()
        region_stats["Mobile %"] = (region_stats["views_mobile"] / region_stats["video_view"] * 100).round(1)
        region_stats.columns = ["Region", "Views", "Videos", "Completion", "Mobile Views", "Mobile %"]
        region_stats["Completion_Num"] = region_stats["Completion"] * 100
        region_stats["Completion"] = region_stats["Completion_Num"].round(1).astype(str) + "%"
        region_stats = region_stats.sort_values("Views", ascending=False)
        df = region_stats[["Region", "Videos", "Views", "Completion", "Mobile %"]]

        top_region = df.iloc[0]['Region']
        top_views = df.iloc[0]['Views']

        # Get APAC mobile stats
        apac_mobile = df[df['Region']=='APAC']['Mobile %'].iloc[0] if 'APAC' in df['Region'].values else 35
        americas_mobile = df[df['Region']=='Americas']['Mobile %'].iloc[0] if 'Americas' in df['Region'].values else 28
        mobile_gap = apac_mobile - americas_mobile

        # Calculate potential
        apac_views = df[df['Region']=='APAC']['Views'].iloc[0] if 'APAC' in df['Region'].values else 0
        potential_mobile_impact = int(apac_views * 0.15)  # 15% improvement potential

        summary = f"""**Regional Leader:** {top_region} with {top_views:,} views ({top_views/total_views*100:.0f}% of total).

**Mobile Opportunity:** APAC mobile usage ({apac_mobile:.0f}%) is {mobile_gap:.0f}pp higher than Americas ({americas_mobile:.0f}%). Industry benchmark: {BENCHMARKS['mobile_share']['industry']}%.

**Recommended Actions:**
1. **APAC mobile-first:** Add captions to top 10 APAC videos (mobile viewers often watch muted)
2. **Localization:** Create region-specific thumbnails and titles for EMEA markets
3. **Timing optimization:** Align publish times with regional peak hours (APAC: 9am SGT, EMEA: 9am CET)

**Owner:** Regional Marketing Leads | **Impact:** +{potential_mobile_impact:,} completions from mobile optimization"""

        return df, "bar", summary

    elif query_type == "quarterly_yoy":
        # Filter for quarterly results content
        quarterly_dims = dimensions_df[dimensions_df["video_content_type"] == "Quarterly Results"]
        # Only get division_name from dimensions (facts already has year/quarter)
        merged = facts_df.merge(quarterly_dims[["video_id", "division_name"]], on="video_id")

        # Use year/quarter from facts_df (already present)
        yoy_stats = merged.groupby(["year", "quarter"]).agg({
            "video_view": "sum",
            "video_engagement_100": "mean"
        }).reset_index()
        yoy_stats.columns = ["Year", "Quarter", "Views", "Completion"]
        yoy_stats["Completion_Num"] = yoy_stats["Completion"] * 100
        yoy_stats["Completion"] = yoy_stats["Completion_Num"].round(1).astype(str) + "%"
        yoy_stats["Period"] = yoy_stats["Quarter"] + " " + yoy_stats["Year"].astype(str)
        yoy_stats = yoy_stats.sort_values(["Year", "Quarter"])

        # Calculate YoY change
        views_2024 = yoy_stats[yoy_stats["Year"] == 2024]["Views"].sum()
        views_2025 = yoy_stats[yoy_stats["Year"] == 2025]["Views"].sum()
        yoy_change = ((views_2025 - views_2024) / views_2024 * 100) if views_2024 > 0 else 0
        view_diff = views_2025 - views_2024

        # Find best performing quarter
        best_quarter = yoy_stats.loc[yoy_stats["Views"].idxmax()]

        # Get Q4 specific stats (typically earnings season peak)
        q4_data = yoy_stats[yoy_stats["Quarter"] == "Q4"]
        q4_avg = q4_data["Views"].mean() if len(q4_data) > 0 else 0
        other_avg = yoy_stats[yoy_stats["Quarter"] != "Q4"]["Views"].mean()
        q4_lift = ((q4_avg - other_avg) / other_avg * 100) if other_avg > 0 else 0

        trend_word = "increased" if yoy_change > 0 else "decreased"
        trend_emoji = "📈" if yoy_change > 0 else "📉"

        summary = f"""{trend_emoji} **YoY Performance:** Quarterly results viewership {trend_word} **{abs(yoy_change):.1f}%** ({view_diff:+,} views).

**Peak Performance:** {best_quarter['Period']} achieved {best_quarter['Views']:,} views with {best_quarter['Completion']} completion.

**Q4 Earnings Effect:** Q4 videos average {q4_lift:+.0f}% higher views than other quarters.

**Recommended Actions:**
1. **Pre-release promotion:** Start earnings video promotion 48hrs before publish (currently same-day)
2. **Extend Q4 format:** Apply Q4 production quality to Q1-Q3 releases
3. **Executive visibility:** Feature CEO intro in all quarterly videos (currently 60% coverage)

**Owner:** Investor Relations + Corp Comms | **Target:** +25% YoY for Q1 2026 earnings"""

        # Create display dataframe for chart
        df_display = yoy_stats[["Period", "Views", "Completion"]].copy()
        return df_display, "bar", summary

    elif query_type == "funnel":
        funnel_data = pd.DataFrame({
            "stage": ["Started", "25% Watched", "50% Watched", "75% Watched", "Completed"],
            "retention_pct": [
                facts_df["video_engagement_1"].mean() * 100,
                facts_df["video_engagement_25"].mean() * 100,
                facts_df["video_engagement_50"].mean() * 100,
                facts_df["video_engagement_75"].mean() * 100,
                facts_df["video_engagement_100"].mean() * 100,
            ]
        })
        funnel_data["retention_pct"] = funnel_data["retention_pct"].round(1)

        start_pct = funnel_data.iloc[0]["retention_pct"]
        completion_pct = funnel_data.iloc[-1]["retention_pct"]
        drop_25 = start_pct - funnel_data.iloc[1]["retention_pct"]
        drop_50 = funnel_data.iloc[1]["retention_pct"] - funnel_data.iloc[2]["retention_pct"]
        drop_75 = funnel_data.iloc[2]["retention_pct"] - funnel_data.iloc[3]["retention_pct"]

        # Find biggest drop-off point
        drops = [("0-25%", drop_25), ("25-50%", drop_50), ("50-75%", drop_75)]
        biggest_drop = max(drops, key=lambda x: x[1])

        # Calculate potential recovery
        total_views = facts_df["video_view"].sum()
        potential_completions = int(total_views * (biggest_drop[1] / 100) * 0.2)  # 20% recovery potential

        summary = f"""**Funnel Analysis:** {start_pct:.0f}% start → {completion_pct:.0f}% complete ({completion_pct/start_pct*100:.0f}% conversion rate).

**Critical Drop-off:** The {biggest_drop[0]} segment loses **{biggest_drop[1]:.1f}%** of viewers—your biggest opportunity.

**Benchmark:** Industry avg completion is {BENCHMARKS['completion_rate']['industry']}%. You're at {completion_pct:.1f}%.

**Recommended Actions:**
1. **Hook optimization:** Add pattern interrupt at 15-second mark (biggest early drop-off point)
2. **Chapter markers:** Implement chapters at 25%, 50%, 75% to enable skip-to-relevant content
3. **End card CTAs:** Add "Watch next" prompts at 85% mark to capture abandoning viewers
4. **A/B test:** Run thumbnail tests on top 5 videos—industry sees 12% lift from optimization

**Owner:** Video Production | **Potential Impact:** Recovering 20% of {biggest_drop[0]} drop-off = +{potential_completions:,} completions"""

        return funnel_data, "funnel", summary

    elif query_type == "devices":
        # Group by division to show device breakdown per business unit
        merged = facts_df.merge(dimensions_df[["video_id", "region", "division_name"]], on="video_id")
        device_by_division = merged.groupby("division_name").agg({
            "views_desktop": "sum",
            "views_mobile": "sum",
            "views_tablet": "sum",
            "views_other": "sum",
            "video_view": "sum"
        }).reset_index()
        device_by_division["Desktop %"] = (device_by_division["views_desktop"] / device_by_division["video_view"] * 100).round(1)
        device_by_division["Mobile %"] = (device_by_division["views_mobile"] / device_by_division["video_view"] * 100).round(1)
        device_by_division["Tablet %"] = (device_by_division["views_tablet"] / device_by_division["video_view"] * 100).round(1)
        device_by_division["Other %"] = (device_by_division["views_other"] / device_by_division["video_view"] * 100).round(1)
        device_by_division["Total Views"] = device_by_division["video_view"]
        df = device_by_division[["division_name", "Desktop %", "Mobile %", "Tablet %", "Other %", "Total Views"]]
        df.columns = ["Division", "Desktop %", "Mobile %", "Tablet %", "Other %", "Total Views"]

        # Extract key metrics for summary
        gwm_row = df[df['Division']=='Global Wealth Management']
        gwm_tablet = gwm_row['Tablet %'].iloc[0] if len(gwm_row) > 0 else 0
        gwm_mobile = gwm_row['Mobile %'].iloc[0] if len(gwm_row) > 0 else 0
        gwm_desktop = gwm_row['Desktop %'].iloc[0] if len(gwm_row) > 0 else 0

        # Industry benchmark
        industry_mobile = BENCHMARKS['mobile_share']['industry']
        mobile_trend = BENCHMARKS['mobile_share']['trend']

        total_mobile = merged["views_mobile"].sum()
        total_tablet = merged["views_tablet"].sum()
        total_views = merged["video_view"].sum()
        overall_mobile = total_mobile / total_views * 100
        overall_tablet = total_tablet / total_views * 100

        summary = f"""**Device Distribution by Division:** Tablet accounts for **{overall_tablet:.1f}%** of all views, Mobile **{overall_mobile:.0f}%** (industry mobile: {industry_mobile}%).

**Global Wealth Management (GWM) Breakdown:**
- **Desktop:** {gwm_desktop:.1f}%
- **Mobile:** {gwm_mobile:.1f}%
- **Tablet:** {gwm_tablet:.1f}%

**Key Insight:** Wealth management clients often use tablets for reviewing portfolio content during meetings.

**Recommended Actions:**
1. **Tablet optimization:** Ensure GWM videos render well on iPad (preferred device for client meetings)
2. **Mobile captions:** Add burned-in captions for on-the-go viewing
3. **Duration by device:** Consider shorter cuts (< 3 min) for mobile, longer deep-dives for tablet

**Owner:** Video Production + GWM Digital Team | **Priority:** High for client-facing content"""

        return df, "stacked_bar", summary

    elif query_type == "channels":
        merged = facts_df.merge(dimensions_df[["video_id", "channel", "division_name"]], on="video_id")
        channel_stats = merged.groupby(["channel", "division_name"]).agg({
            "video_view": "sum",
            "video_id": "nunique",
            "video_engagement_100": "mean"
        }).reset_index()
        channel_stats["Completion_Num"] = channel_stats["video_engagement_100"] * 100
        channel_stats.columns = ["Channel", "Division", "Views", "Videos", "Completion_Num"]
        channel_stats["Completion"] = channel_stats["Completion_Num"].round(1).astype(str) + "%"
        channel_stats = channel_stats.sort_values("Views", ascending=False).head(10)

        top_channel = channel_stats.iloc[0]["Channel"]
        top_division = channel_stats.iloc[0]["Division"]
        top_views = channel_stats.iloc[0]["Views"]

        # Find underperforming channels
        avg_completion = channel_stats["Completion_Num"].mean()
        underperformers = channel_stats[channel_stats["Completion_Num"] < avg_completion * 0.8]

        summary = f"""**Channel Performance Analysis:**

**Top Performer:** {top_channel} ({top_division}) leads with {top_views:,} views.

**Distribution:** Top 3 channels account for {channel_stats.head(3)['Views'].sum()/channel_stats['Views'].sum()*100:.0f}% of all channel views.

**Attention Needed:** {len(underperformers)} channels have below-average completion rates.

**Recommended Actions:**
1. **Best practice sharing:** Document {top_channel}'s content approach for other channels
2. **Channel consolidation:** Consider merging low-volume channels to focus resources
3. **Audience segmentation:** Create channel-specific content calendars based on viewer behavior
4. **Cross-promotion:** Feature top content from smaller channels on main channels

**Owner:** Channel Managers | **Review:** Monthly channel performance review"""

        df_display = channel_stats[["Channel", "Division", "Videos", "Views", "Completion"]]
        return df_display, "bar", summary

    elif query_type == "trends":
        monthly = facts_df.groupby(facts_df["date"].dt.to_period("M")).agg({
            "video_view": "sum",
            "video_id": "nunique",
            "engagement_score": "mean"
        }).reset_index()
        monthly["date"] = monthly["date"].dt.to_timestamp()
        monthly.columns = ["Month", "Views", "Active Videos", "Engagement"]
        monthly["Engagement"] = (monthly["Engagement"] * 100).round(1)

        # Calculate trend
        first_half = monthly["Views"].iloc[:len(monthly)//2].mean() if len(monthly) > 1 else monthly["Views"].iloc[0]
        second_half = monthly["Views"].iloc[len(monthly)//2:].mean() if len(monthly) > 1 else monthly["Views"].iloc[-1]
        trend_pct = ((second_half - first_half) / first_half * 100) if first_half > 0 else 0
        trend = "increased" if trend_pct > 0 else "decreased"
        trend_emoji = "📈" if trend_pct > 0 else "📉"

        best_month = monthly.loc[monthly["Views"].idxmax(), "Month"].strftime("%B %Y")
        best_views = monthly["Views"].max()
        worst_month = monthly.loc[monthly["Views"].idxmin(), "Month"].strftime("%B %Y")
        worst_views = monthly["Views"].min()
        volatility = (best_views - worst_views) / worst_views * 100 if worst_views > 0 else 0

        # Predict next month (simple)
        recent_avg = monthly["Views"].tail(3).mean()
        projected = int(recent_avg * 1.05)  # Conservative 5% growth

        summary = f"""{trend_emoji} **Trend:** Views have {trend} **{abs(trend_pct):.1f}%** over the period.

**Peak & Trough:**
- Best: **{best_month}** ({best_views:,} views)
- Lowest: **{worst_month}** ({worst_views:,} views)
- Volatility: {volatility:.0f}% swing

**Projection:** Based on recent 3-month average, expect ~{projected:,} views next month.

**Seasonal Patterns:** Quarterly earnings (Feb, May, Aug, Nov) drive predictable 30-40% spikes.

**Recommended Actions:**
1. **Content calendar:** Align major releases with historical peak periods
2. **Pre-schedule:** Promote upcoming earnings content 1 week in advance
3. **Fill troughs:** Schedule evergreen content during historically low periods
4. **Baseline tracking:** Set {int(monthly['Views'].median()):,} views/month as baseline target

**Owner:** Content Planning | **Next Review:** Monthly performance check-in"""

        return monthly, "line", summary

    elif query_type == "content_types":
        merged = facts_df.merge(dimensions_df[["video_id", "video_content_type", "video_duration_seconds"]], on="video_id")
        type_stats = merged.groupby("video_content_type").agg({
            "video_view": "sum",
            "video_id": "nunique",
            "video_duration_seconds": "mean",
            "video_engagement_100": "mean"
        }).reset_index()
        type_stats.columns = ["Content Type", "Views", "Videos", "Avg Duration (min)", "Completion_Num"]
        type_stats["Avg Duration (min)"] = (type_stats["Avg Duration (min)"] / 60).round(1)
        type_stats["Completion"] = (type_stats["Completion_Num"] * 100).round(1).astype(str) + "%"
        type_stats = type_stats.sort_values("Views", ascending=False)

        top_type = type_stats.iloc[0]["Content Type"]
        top_views = type_stats.iloc[0]["Views"]
        total_views = type_stats["Views"].sum()

        # Find best completion
        best_completion_type = type_stats.loc[type_stats["Completion_Num"].idxmax(), "Content Type"]
        best_completion = type_stats["Completion_Num"].max() * 100

        # Find efficiency (views per video)
        type_stats["Efficiency"] = type_stats["Views"] / type_stats["Videos"]
        most_efficient = type_stats.loc[type_stats["Efficiency"].idxmax(), "Content Type"]

        summary = f"""**Content Performance Overview:**

**Volume Leader:** {top_type} drives {top_views:,} views ({top_views/total_views*100:.0f}% of total).

**Engagement Leader:** {best_completion_type} achieves {best_completion:.1f}% completion rate.

**Efficiency Winner:** {most_efficient} content has highest views-per-video ratio.

**Strategic Recommendations:**
1. **Resource allocation:** Increase {top_type} production if engagement metrics hold
2. **Format transfer:** Apply {best_completion_type}'s engagement tactics to {type_stats.iloc[-1]['Content Type']}
3. **Content mix:** Current split is optimized for reach; consider rebalancing for engagement
4. **Sunset review:** Evaluate lowest-performing category for consolidation

**Owner:** Content Strategy | **Review Cycle:** Quarterly content mix assessment"""

        df_display = type_stats[["Content Type", "Videos", "Views", "Avg Duration (min)", "Completion"]]
        return df_display, "bar", summary

    elif query_type == "training_compliance":
        training_dims = dimensions_df[dimensions_df["video_content_type"].isin(["Training", "Compliance"])]
        merged = facts_df.merge(training_dims[["video_id", "name", "division_name", "region"]], on="video_id")
        training_stats = merged.groupby(["name", "division_name", "region"]).agg({
            "video_view": "sum",
            "video_engagement_100": "mean"
        }).reset_index()
        training_stats["Completion_Num"] = training_stats["video_engagement_100"] * 100
        training_stats.columns = ["Video", "Division", "Region", "Views", "Completion_Num"]
        training_stats["Completion"] = training_stats["Completion_Num"].round(1).astype(str) + "%"
        training_stats = training_stats.sort_values("Views", ascending=False).head(10)

        avg_completion = merged["video_engagement_100"].mean() * 100
        target_completion = 70.0  # Typical compliance target
        compliance_gap = avg_completion - target_completion

        # Compare to non-training
        non_training_dims = dimensions_df[~dimensions_df["video_content_type"].isin(["Training", "Compliance"])]
        non_training_facts = facts_df[facts_df["video_id"].isin(non_training_dims["video_id"])]
        non_training_completion = non_training_facts["video_engagement_100"].mean() * 100 if len(non_training_facts) > 0 else 35

        completion_lift = avg_completion - non_training_completion

        # Find below-target videos
        below_target = training_stats[training_stats["Completion_Num"] < target_completion]
        below_target_count = len(below_target)

        status_emoji = "✅" if avg_completion >= target_completion else "⚠️"

        summary = f"""{status_emoji} **Training Completion:** {avg_completion:.1f}% average (target: {target_completion:.0f}%)

**Performance vs Other Content:** Training videos achieve **+{completion_lift:.1f}pp** higher completion than non-training content ({non_training_completion:.1f}%).

**Compliance Status:**
- Videos meeting 70% target: {len(training_stats) - below_target_count} of {len(training_stats)}
- Videos below target: {below_target_count}

**Best Practices to Transfer:**
1. **Mandatory framing:** Training's "required viewing" language drives 2x completion
2. **Progress tracking:** Add visible progress bars to all strategic content
3. **Completion certificates:** Offer recognition for completing leadership content

**Recommended Actions:**
1. {"Investigate below-target videos for content issues" if below_target_count > 0 else "Maintain current approach - exceeding targets"}
2. Apply training video format to CEO Town Halls
3. Add completion tracking to quarterly results videos

**Owner:** L&D + Corp Comms | **Risk Level:** {"Low" if avg_completion >= target_completion else "Medium - regulatory compliance"}"""

        df_display = training_stats[["Video", "Division", "Region", "Views", "Completion"]]
        return df_display, "bar", summary

    elif query_type == "watch_time":
        merged = facts_df.merge(dimensions_df[["video_id", "division_name", "region"]], on="video_id")
        watch_stats = merged.groupby(["division_name", "region"]).agg({
            "video_seconds_viewed": "sum",
            "video_view": "sum"
        }).reset_index()
        watch_stats["Watch Hours"] = (watch_stats["video_seconds_viewed"] / 3600).round(0).astype(int)
        watch_stats["Avg Min/View"] = (watch_stats["video_seconds_viewed"] / watch_stats["video_view"] / 60).round(1)
        watch_stats.columns = ["Division", "Region", "Seconds", "Views", "Watch Hours", "Avg Min/View"]
        watch_stats = watch_stats.sort_values("Watch Hours", ascending=False)
        df = watch_stats[["Division", "Region", "Views", "Watch Hours", "Avg Min/View"]]

        total_hours = df["Watch Hours"].sum()
        top_div = df.iloc[0]["Division"]
        top_region = df.iloc[0]["Region"]
        top_hours = df.iloc[0]["Watch Hours"]

        # Calculate engagement depth
        best_depth = df.loc[df["Avg Min/View"].idxmax()]
        industry_avg_watch = BENCHMARKS["avg_watch_time_min"]["industry"]

        # Business value estimation (assuming $50/hour employee time)
        if total_hours > 0:
            equivalent_fte = total_hours / 2080  # Annual working hours
            value_estimate = total_hours * 50

        summary = f"""**Watch Time Analysis:**

**Total Investment:** {total_hours:,} hours of employee attention invested in video content.

**Top Consumer:** {top_div} ({top_region}) with {top_hours:,} hours—indicates deep content engagement.

**Engagement Depth:** {best_depth['Division']} ({best_depth['Region']}) averages {best_depth['Avg Min/View']:.1f} min/view (industry: {industry_avg_watch} min).

**Business Impact:**
- Equivalent to **{equivalent_fte:.1f} FTE** annual workload
- Estimated knowledge transfer value: **${value_estimate:,.0f}** (at $50/hr)

**Recommended Actions:**
1. **ROI tracking:** Implement post-video surveys to measure knowledge retention
2. **Time optimization:** Consider 1.5x playback option for training content
3. **Highlight creation:** Extract key moments from long-form content for quick reference
4. **Analytics integration:** Connect watch time to business outcomes (e.g., compliance scores)

**Owner:** L&D Analytics | **Impact Metric:** Knowledge transfer efficiency"""

        return df, "bar", summary

    elif query_type == "correlation":
        # Correlation analysis: Duration vs Completion
        merged = facts_df.merge(
            dimensions_df[["video_id", "video_duration_seconds", "video_content_type"]],
            on="video_id"
        )
        video_stats = merged.groupby(["video_id", "video_duration_seconds", "video_content_type"]).agg({
            "video_view": "sum",
            "video_engagement_100": "mean"
        }).reset_index()

        video_stats["Duration (min)"] = (video_stats["video_duration_seconds"] / 60).round(1)
        video_stats["Completion %"] = (video_stats["video_engagement_100"] * 100).round(1)
        video_stats["Views"] = video_stats["video_view"]

        df = video_stats[["Duration (min)", "Completion %", "Views", "video_content_type"]]
        df.columns = ["Duration (min)", "Completion %", "Views", "Content Type"]

        # Calculate correlation
        corr = df["Duration (min)"].corr(df["Completion %"])
        corr_direction = "negative" if corr < 0 else "positive"
        corr_strength = "strong" if abs(corr) > 0.5 else "moderate" if abs(corr) > 0.3 else "weak"

        short_completion = df[df['Duration (min)'] < 10]['Completion %'].mean()
        long_completion = df[df['Duration (min)'] >= 10]['Completion %'].mean()
        completion_gap = short_completion - long_completion

        # Find optimal duration
        df_bucketed = df.copy()
        df_bucketed['Duration Bucket'] = pd.cut(df['Duration (min)'], bins=[0, 3, 5, 10, 20, 100],
                                                 labels=['0-3 min', '3-5 min', '5-10 min', '10-20 min', '20+ min'])
        optimal_bucket = df_bucketed.groupby('Duration Bucket')['Completion %'].mean().idxmax()

        # Count long videos that could be split
        long_videos = len(df[df['Duration (min)'] >= 15])
        potential_gain = int(long_videos * 0.3 * df['Views'].mean() * (completion_gap/100))

        summary = f"""**Correlation Found:** {corr_strength.title()} {corr_direction} correlation (r={corr:.2f}) between duration and completion.

**Key Finding:**
- Short videos (<10 min): **{short_completion:.1f}%** avg completion
- Long videos (≥10 min): **{long_completion:.1f}%** avg completion
- **Gap: {completion_gap:.1f} percentage points**

**Optimal Duration:** Videos in the **{optimal_bucket}** range show highest completion rates.

**⚠️ Caveat:** Correlation ≠ causation. Training videos may be longer AND have mandatory completion requirements.

**Recommended Actions:**
1. **Segment long content:** Break {long_videos} videos over 15 min into chapter-based series
2. **Duration guidelines:** Set {optimal_bucket} as default target for new content
3. **Exception handling:** Keep mandatory training as single videos but add progress saving

**Owner:** Content Standards Team | **Potential Impact:** +{potential_gain:,} additional completions"""

        return df, "scatter", summary

    elif query_type == "histogram":
        # Distribution of completion rates
        merged = facts_df.merge(
            dimensions_df[["video_id", "name", "video_content_type"]],
            on="video_id"
        )
        video_stats = merged.groupby(["video_id", "name", "video_content_type"]).agg({
            "video_engagement_100": "mean",
            "video_view": "sum"
        }).reset_index()
        video_stats["Completion %"] = (video_stats["video_engagement_100"] * 100).round(1)
        video_stats = video_stats.rename(columns={
            "name": "Video",
            "video_content_type": "Content Type",
            "video_view": "Views"
        })

        # Calculate distribution stats
        mean_completion = video_stats["Completion %"].mean()
        median_completion = video_stats["Completion %"].median()
        std_completion = video_stats["Completion %"].std()
        below_benchmark = (video_stats["Completion %"] < BENCHMARKS['completion_rate']['industry']).sum()
        above_benchmark = (video_stats["Completion %"] >= BENCHMARKS['completion_rate']['industry']).sum()

        # Identify clusters
        low_performers = len(video_stats[video_stats["Completion %"] < 20])
        high_performers = len(video_stats[video_stats["Completion %"] >= 50])

        summary = f"""**Distribution Analysis:** Your {len(video_stats)} videos show a **{std_completion:.1f}% standard deviation** in completion rates.

**Key Stats:**
- Mean: **{mean_completion:.1f}%** | Median: **{median_completion:.1f}%**
- Below industry benchmark ({BENCHMARKS['completion_rate']['industry']}%): **{below_benchmark}** videos
- Above industry benchmark: **{above_benchmark}** videos

**Clusters Identified:**
- 🔴 **Low performers** (<20% completion): {low_performers} videos - need immediate attention
- 🟢 **High performers** (≥50% completion): {high_performers} videos - study for best practices

**Recommended Actions:**
1. **Audit low performers:** Review the {low_performers} videos under 20% completion for common issues
2. **Success patterns:** Analyze what makes your top {high_performers} videos successful
3. **Standardize:** Apply learnings to bring median closer to mean

**Owner:** Content Quality Team | **Goal:** Reduce standard deviation by 25%"""

        return video_stats[["Video", "Content Type", "Views", "Completion %"]], "histogram", summary

    elif query_type == "outliers":
        # Quadrant analysis: Duration vs Completion
        merged = facts_df.merge(
            dimensions_df[["video_id", "name", "video_duration_seconds", "video_content_type"]],
            on="video_id"
        )
        video_stats = merged.groupby(["video_id", "name", "video_duration_seconds", "video_content_type"]).agg({
            "video_engagement_100": "mean",
            "video_view": "sum"
        }).reset_index()

        video_stats["Duration (min)"] = (video_stats["video_duration_seconds"] / 60).round(1)
        video_stats["Completion %"] = (video_stats["video_engagement_100"] * 100).round(1)
        video_stats = video_stats.rename(columns={
            "name": "Video",
            "video_content_type": "Content Type",
            "video_view": "Views"
        })

        # Calculate thresholds (median-based)
        duration_threshold = video_stats["Duration (min)"].median()
        completion_threshold = video_stats["Completion %"].median()

        # Identify problem videos (long + low completion)
        problem_mask = (video_stats["Duration (min)"] >= duration_threshold) & (video_stats["Completion %"] < completion_threshold)
        problem_videos = video_stats[problem_mask]

        # Identify star performers (any duration + high completion)
        star_mask = video_stats["Completion %"] >= completion_threshold
        star_videos = video_stats[star_mask]

        # Efficient short videos
        efficient_mask = (video_stats["Duration (min)"] < duration_threshold) & (video_stats["Completion %"] >= completion_threshold)
        efficient_videos = video_stats[efficient_mask]

        summary = f"""**Quadrant Analysis:** Identifying outliers based on Duration vs Completion.

**Thresholds Used:**
- Duration threshold: **{duration_threshold:.1f} min** (median)
- Completion threshold: **{completion_threshold:.0f}%** (median)

**Quadrant Breakdown:**
- ⚠️ **Problem Videos** (Long + Low Completion): **{len(problem_videos)}** - these need editing or splitting
- ✓ **Efficient Short Videos** (Short + High Completion): **{len(efficient_videos)}** - your best format
- ⭐ **High Performers** (Above completion threshold): **{len(star_videos)}** total

**Top Problem Videos to Fix:**
{chr(10).join([f"- {row['Video'][:40]}... ({row['Duration (min)']:.0f} min, {row['Completion %']:.0f}%)" for _, row in problem_videos.nlargest(3, 'Views').iterrows()]) if len(problem_videos) > 0 else "- None identified!"}

**Recommended Actions:**
1. **Split long content:** Break down the {len(problem_videos)} problem videos into shorter segments
2. **Replicate success:** Study the {len(efficient_videos)} efficient short videos for format patterns
3. **Set guidelines:** Recommend {duration_threshold:.0f} min as maximum length for new content

**Owner:** Content Production | **Potential Impact:** Fix {len(problem_videos)} problem videos → +{int(len(problem_videos) * 500):,} estimated completions"""

        return video_stats[["Video", "Content Type", "Duration (min)", "Completion %", "Views"]], "quadrant_scatter", summary

    # Default fallback
    return pd.DataFrame(), "table", "Analysis complete."


def handle_chart_type_change(user_query: str, last_result: dict, facts_df, dimensions_df):
    """Handle chart type change request by re-rendering previous data with new chart type."""
    if last_result is None:
        # No previous query to re-render - fall back to top videos
        return generate_demo_result("top_videos", facts_df, dimensions_df)

    # Get the requested new chart type
    new_chart_type = parse_chart_type_request(user_query)

    # Get the previous data
    prev_df = last_result.get("dataframe")
    prev_query_type = last_result.get("query_type")
    prev_summary = last_result.get("summary", "Visualization updated.")

    # If no previous dataframe, regenerate from the previous query type
    if prev_df is None or len(prev_df) == 0:
        prev_df, _, prev_summary = generate_demo_result(prev_query_type, facts_df, dimensions_df)

    # Update summary to mention chart change
    new_summary = f"Changed to **{new_chart_type} chart**. {prev_summary}"

    return prev_df, new_chart_type, new_summary


# =============================================================================
# SIDEBAR
# =============================================================================

with st.sidebar:
    # Logo area
    st.markdown(f"""
    <div style="margin-bottom: 1.5rem;">
        <span style="font-size: 1.5rem; color: {COLORS['accent']};">◆</span>
        <span style="font-size: 1.1rem; font-weight: 600; color: {COLORS['black']}; margin-left: 0.5rem;">Analytics</span>
    </div>
    """, unsafe_allow_html=True)

    # Connection status with data freshness
    st.markdown('<div class="sidebar-title">Data Status</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<span class="status-pill status-connected">● Connected</span>', unsafe_allow_html=True)
    with col2:
        st.markdown('<span class="status-pill status-demo">Preview</span>', unsafe_allow_html=True)

    num_videos = len(st.session_state.demo_dimensions)
    num_records = len(st.session_state.demo_facts)

    # Data freshness
    st.markdown(f"""
    <div class="data-freshness" style="margin-top: 0.5rem;">
        <span class="freshness-dot"></span>
        Last updated: {st.session_state.data_timestamp.strftime('%b %d, %Y %H:%M')}
    </div>
    """, unsafe_allow_html=True)
    st.caption(f"{num_videos} videos · {num_records:,} records")

    st.markdown("---")

    # Quick stats with benchmarks
    st.markdown('<div class="sidebar-title">Performance Summary</div>', unsafe_allow_html=True)

    total_views = st.session_state.demo_facts["video_view"].sum()
    avg_completion = st.session_state.demo_facts["video_engagement_100"].mean() * 100
    benchmark_completion = BENCHMARKS["completion_rate"]["industry"]
    completion_vs_benchmark = avg_completion - benchmark_completion

    st.metric("Total Views", f"{total_views:,}", "+12.5% YoY")
    st.metric(
        "Avg Completion",
        f"{avg_completion:.1f}%",
        f"{completion_vs_benchmark:+.1f}% vs industry",
        delta_color="normal" if completion_vs_benchmark >= 0 else "inverse"
    )

    watch_hours = st.session_state.demo_facts["video_seconds_viewed"].sum() / 3600
    st.metric("Watch Hours", f"{watch_hours:,.0f}")

    st.markdown("---")

    # Recommended Actions section
    st.markdown('<div class="sidebar-title">Recommended Actions</div>', unsafe_allow_html=True)

    # Generate actions if not already done
    if st.session_state.recommended_actions is None:
        st.session_state.recommended_actions = generate_recommended_actions(
            st.session_state.demo_facts,
            st.session_state.demo_dimensions
        )

    for i, action in enumerate(st.session_state.recommended_actions[:3]):
        impact_class = f"impact-{action['impact'].lower()}"
        effort_class = f"effort-{action['effort'].lower()}"
        st.markdown(f"""
        <div class="action-item">
            <div class="action-item-title">{action['title']}</div>
            <div class="action-item-desc">{action['description'][:100]}{'...' if len(action['description']) > 100 else ''}</div>
            <div class="action-item-badges">
                <span class="action-badge {impact_class}">Impact: {action['impact']}</span>
                <span class="action-badge {effort_class}">Effort: {action['effort']}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Popular questions
    st.markdown('<div class="sidebar-title">Quick Queries</div>', unsafe_allow_html=True)

    popular_questions = [
        ("📊", "Top performing videos", "top_videos"),
        ("🏛️", "Division performance", "division_performance"),
        ("🌍", "Regional comparison", "regional_performance"),
        ("📅", "Quarterly results YoY", "quarterly_yoy"),
        ("📉", "Engagement funnel", "funnel"),
        ("📱", "Device breakdown", "devices"),
        ("📈", "Completion distribution", "histogram"),
        ("🎯", "Outlier analysis", "outliers"),
    ]

    for icon, label, query_type in popular_questions:
        if st.button(f"{icon}  {label}", key=f"popular_{query_type}", use_container_width=True):
            st.session_state.pending_query = (label, query_type)
            st.rerun()

    st.markdown("---")

    # Export and utilities
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📥 Export", use_container_width=True, help="Export insights to PDF/Excel"):
            st.toast("Export feature coming soon!", icon="📥")
    with col2:
        if st.button("🗑️ Clear", use_container_width=True):
            st.session_state.demo_messages = []
            st.session_state.last_result = None
            st.rerun()

    # Schema viewer
    with st.expander("View data schema"):
        st.code("""
facts (daily metrics)
├── video_id, date, year, quarter
├── video_view, views_desktop/mobile/tablet
├── engagement_score, play_rate
└── video_engagement_1/25/50/75/100

dimensions (video metadata)
├── video_id, name, channel
├── division, division_name
├── region (APAC, EMEA, Americas)
├── video_content_type, video_duration
└── language
        """, language="text")


# =============================================================================
# MAIN CONTENT
# =============================================================================

# Header
st.markdown(f"""
<div class="main-header">
    <h1>
        <span class="accent-diamond">◆</span>
        {APP_TITLE}
        <span class="demo-indicator"><span class="demo-dot"></span>Preview</span>
    </h1>
    <p>AI-powered insights for your video communications</p>
</div>
""", unsafe_allow_html=True)

# Display conversation
for i, message in enumerate(st.session_state.demo_messages):
    if message["role"] == "user":
        st.markdown(f"""
        <div class="user-message">
            <strong>Q:</strong> {message["content"]}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown('<div class="assistant-response">', unsafe_allow_html=True)

        # Show SQL in expander
        if "sql" in message:
            with st.expander("View generated SQL", expanded=False):
                st.code(message["sql"], language="sql")

        # Show chart if available
        if "chart" in message and message["chart"] is not None:
            st.plotly_chart(message["chart"], use_container_width=True, key=f"chart_{i}")

        # Show data table
        if "dataframe" in message and message["dataframe"] is not None and len(message["dataframe"]) > 0:
            st.dataframe(message["dataframe"], use_container_width=True, hide_index=True)

        # Show insight with proper markdown rendering
        if "summary" in message:
            st.markdown(f"""
            <div class="actionable-insight">
                <div class="actionable-insight-header">
                    <h4>Analysis & Recommendations</h4>
                    <span class="benchmark-tag">AI-Generated Insight</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.markdown(message["summary"])

        st.markdown('</div>', unsafe_allow_html=True)

# Handle pending query from sidebar
if "pending_query" in st.session_state:
    query_text, query_type = st.session_state.pending_query
    del st.session_state.pending_query

    # Add user message
    st.session_state.demo_messages.append({"role": "user", "content": query_text})

    # Handle chart type change vs regular query
    if query_type == "chart_change":
        df, chart_type, summary = handle_chart_type_change(
            query_text,
            st.session_state.last_result,
            st.session_state.demo_facts,
            st.session_state.demo_dimensions
        )
        config = {"sql": "-- Chart type changed to: " + chart_type}
    else:
        config = get_demo_response(query_type)
        df, chart_type, summary = generate_demo_result(
            query_type,
            st.session_state.demo_facts,
            st.session_state.demo_dimensions
        )

    # Create chart based on type and available data
    chart = None
    if chart_type == "funnel" and len(df) > 0:
        total_views = st.session_state.demo_facts["video_view"].sum()
        chart = create_funnel_chart(df, base_views=min(10000, total_views // 100))
    elif chart_type == "pie" and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            chart = create_pie_chart(df, numeric_cols[0], first_col)
    elif chart_type == "scatter" and len(df) > 0:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) >= 2:
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
            if "Duration (min)" in df.columns:
                x_col = "Duration (min)"
            if "Completion %" in df.columns:
                y_col = "Completion %"
            size_col = "Views" if "Views" in df.columns else None
            color_col = "Content Type" if "Content Type" in df.columns else None
            # Add descriptive title for C-level clarity
            chart_title = "Video Duration vs. Completion Rate" if x_col == "Duration (min)" else ""
            chart = create_scatter_chart(df, x_col, y_col, size_col=size_col, color_col=color_col, title=chart_title)
    elif chart_type == "stacked_bar" and len(df) > 0:
        percent_cols = [c for c in df.columns if '%' in c]
        if percent_cols:
            chart = create_stacked_bar_chart(df, df.columns[0], percent_cols)
        else:
            first_col = df.columns[0]
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                chart = create_bar_chart(df, first_col, numeric_cols[0])
    elif chart_type == "line" and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            chart = create_line_chart(df, first_col, numeric_cols[0])
    elif chart_type == "horizontal_bar" and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            fig = px.bar(df, y=first_col, x=numeric_cols[0], orientation='h',
                         color_discrete_sequence=[COLORS['chart_1']])
            fig.update_layout(
                plot_bgcolor='white', paper_bgcolor='white',
                font_family="Inter", font_color=COLORS['dark'],
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis=dict(showgrid=True, gridcolor=COLORS['border']),
                yaxis=dict(showgrid=False)
            )
            chart = fig
    elif (chart_type == "bar" or chart_type == "grouped_bar") and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            chart = create_bar_chart(df, first_col, numeric_cols[0])
    elif chart_type == "histogram" and len(df) > 0:
        if "Completion %" in df.columns:
            chart = create_histogram_chart(
                df, "Completion %",
                title="Distribution of Video Completion Rates",
                benchmark=BENCHMARKS['completion_rate']['industry']
            )
    elif chart_type == "quadrant_scatter" and len(df) > 0:
        if "Duration (min)" in df.columns and "Completion %" in df.columns:
            chart = create_quadrant_scatter_chart(
                df,
                x_col="Duration (min)",
                y_col="Completion %",
                label_col="Video",
                title="Video Performance Quadrant Analysis"
            )

    response = {
        "role": "assistant",
        "sql": config["sql"],
        "dataframe": df if chart_type not in ["funnel", "histogram", "quadrant_scatter"] else None,
        "chart": chart,
        "summary": summary,
        "query_type": query_type if query_type != "chart_change" else st.session_state.last_result.get("query_type", "top_videos") if st.session_state.last_result else "top_videos",
        "chart_type": chart_type
    }

    # Store result for future chart type changes
    st.session_state.last_result = {
        "dataframe": df,
        "query_type": response["query_type"],
        "chart_type": chart_type,
        "summary": summary
    }

    st.session_state.demo_messages.append(response)
    st.session_state.last_query_type = response["query_type"]
    st.rerun()

# Footer metrics / Landing page
if not st.session_state.demo_messages:
    # Generate proactive insights if not already done
    if st.session_state.proactive_insights is None:
        st.session_state.proactive_insights = generate_proactive_insights(
            st.session_state.demo_facts,
            st.session_state.demo_dimensions
        )

    # Executive Summary Metrics Row
    st.markdown(f"""
    <div class="section-header">
        <h3 class="section-title">Executive Summary</h3>
        <span class="benchmark-tag">vs. Industry Benchmarks</span>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_views = st.session_state.demo_facts["video_view"].sum()
        st.metric("Total Views", f"{total_views:,}", "+12.5% YoY")

    with col2:
        avg_completion = st.session_state.demo_facts["video_engagement_100"].mean() * 100
        benchmark = BENCHMARKS["completion_rate"]["industry"]
        delta = avg_completion - benchmark
        st.metric(
            "Completion Rate",
            f"{avg_completion:.1f}%",
            f"{delta:+.1f}% vs industry ({benchmark}%)",
            delta_color="normal" if delta >= 0 else "inverse"
        )

    with col3:
        watch_hours = st.session_state.demo_facts["video_seconds_viewed"].sum() / 3600
        st.metric("Watch Hours", f"{watch_hours:,.0f}", "+8.3% YoY")

    with col4:
        unique_videos = st.session_state.demo_dimensions["video_id"].nunique()
        active_rate = len(st.session_state.demo_facts["video_id"].unique()) / unique_videos * 100
        st.metric("Active Videos", f"{unique_videos}", f"{active_rate:.0f}% with views")

    st.markdown("<br>", unsafe_allow_html=True)

    # Proactive Insights Section
    st.markdown(f"""
    <div class="section-header">
        <h3 class="section-title">Attention Required</h3>
        <span style="font-size: 0.75rem; color: {COLORS['gray']};">AI-detected insights requiring action</span>
    </div>
    """, unsafe_allow_html=True)

    # Display proactive insight cards
    insights = st.session_state.proactive_insights

    # Show insights in a 2-column layout
    if insights:
        col1, col2 = st.columns(2)

        for i, insight in enumerate(insights):
            with col1 if i % 2 == 0 else col2:
                metric_class = "negative" if insight["type"] in ["warning", "alert"] else "positive"
                priority_class = f"priority-{insight.get('priority', 'medium').lower()}"

                impact_line = ""
                if "impact" in insight:
                    impact_line = f'<br><span style="color: {COLORS["success"]}; font-weight: 500;">📈 {insight["impact"]}</span>'

                card_html = f'''<div class="insight-card {insight['type']}">
<div class="insight-card-header">
<h4 class="insight-card-title">{insight['title']}</h4>
<span class="insight-card-metric {metric_class}">{insight['metric']}</span>
</div>
<p class="insight-card-detail">{insight['detail']}</p>
<div class="insight-card-action">
<strong>Action:</strong> {insight['action']}{impact_line}
</div>
<div class="insight-card-meta">
<span class="{priority_class}">● {insight.get('priority', 'Medium')} Priority</span>
<span>👤 {insight.get('owner', 'TBD')}</span>
</div>
</div>'''
                st.markdown(card_html, unsafe_allow_html=True)
    else:
        st.info("No urgent insights detected. Your video performance is on track!")

# =============================================================================
# SUGGESTIONS & CHAT INPUT (always at bottom)
# =============================================================================

# Get contextual suggestions
last_query = st.session_state.demo_messages[-1].get("query_type") if st.session_state.demo_messages else None
last_chart = st.session_state.demo_messages[-1].get("chart_type") if st.session_state.demo_messages else None
suggestions = get_contextual_suggestions(last_query, last_chart)

# Subtle suggestion prompts with sparkle prefix
st.markdown('<div class="suggestions-label">Try asking</div>', unsafe_allow_html=True)
st.markdown('<div class="suggestion-btn-container">', unsafe_allow_html=True)

# Render suggestion buttons in a row with sparkle prefix
cols = st.columns(len(suggestions))
for idx, suggestion in enumerate(suggestions):
    with cols[idx]:
        if st.button(f"✦ {suggestion}", key=f"suggestion_{idx}"):
            query_type = match_query(suggestion)
            st.session_state.pending_query = (suggestion, query_type)
            st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# Chat input
if prompt := st.chat_input("Ask a question about your video data..."):
    query_type = match_query(prompt)

    # Add user message
    st.session_state.demo_messages.append({"role": "user", "content": prompt})

    # Handle chart type change vs regular query
    if query_type == "chart_change":
        df, chart_type, summary = handle_chart_type_change(
            prompt,
            st.session_state.last_result,
            st.session_state.demo_facts,
            st.session_state.demo_dimensions
        )
        config = {"sql": "-- Chart type changed to: " + chart_type}
    else:
        config = get_demo_response(query_type)
        df, chart_type, summary = generate_demo_result(
            query_type,
            st.session_state.demo_facts,
            st.session_state.demo_dimensions
        )

    # Create chart based on type and available data
    chart = None
    if chart_type == "funnel" and len(df) > 0:
        total_views = st.session_state.demo_facts["video_view"].sum()
        chart = create_funnel_chart(df, base_views=min(10000, total_views // 100))
    elif chart_type == "pie" and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            chart = create_pie_chart(df, numeric_cols[0], first_col)
    elif chart_type == "scatter" and len(df) > 0:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if len(numeric_cols) >= 2:
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
            if "Duration (min)" in df.columns:
                x_col = "Duration (min)"
            if "Completion %" in df.columns:
                y_col = "Completion %"
            size_col = "Views" if "Views" in df.columns else None
            color_col = "Content Type" if "Content Type" in df.columns else None
            # Add descriptive title for C-level clarity
            chart_title = "Video Duration vs. Completion Rate" if x_col == "Duration (min)" else ""
            chart = create_scatter_chart(df, x_col, y_col, size_col=size_col, color_col=color_col, title=chart_title)
    elif chart_type == "stacked_bar" and len(df) > 0:
        percent_cols = [c for c in df.columns if '%' in c]
        if percent_cols:
            chart = create_stacked_bar_chart(df, df.columns[0], percent_cols)
        else:
            first_col = df.columns[0]
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                chart = create_bar_chart(df, first_col, numeric_cols[0])
    elif chart_type == "line" and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            chart = create_line_chart(df, first_col, numeric_cols[0])
    elif chart_type == "horizontal_bar" and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            fig = px.bar(df, y=first_col, x=numeric_cols[0], orientation='h',
                         color_discrete_sequence=[COLORS['chart_1']])
            fig.update_layout(
                plot_bgcolor='white', paper_bgcolor='white',
                font_family="Inter", font_color=COLORS['dark'],
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis=dict(showgrid=True, gridcolor=COLORS['border']),
                yaxis=dict(showgrid=False)
            )
            chart = fig
    elif (chart_type == "bar" or chart_type == "grouped_bar") and len(df) > 0:
        first_col = df.columns[0]
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            chart = create_bar_chart(df, first_col, numeric_cols[0])
    elif chart_type == "histogram" and len(df) > 0:
        if "Completion %" in df.columns:
            chart = create_histogram_chart(
                df, "Completion %",
                title="Distribution of Video Completion Rates",
                benchmark=BENCHMARKS['completion_rate']['industry']
            )
    elif chart_type == "quadrant_scatter" and len(df) > 0:
        if "Duration (min)" in df.columns and "Completion %" in df.columns:
            chart = create_quadrant_scatter_chart(
                df,
                x_col="Duration (min)",
                y_col="Completion %",
                label_col="Video",
                title="Video Performance Quadrant Analysis"
            )

    response = {
        "role": "assistant",
        "sql": config["sql"],
        "dataframe": df if chart_type not in ["funnel", "histogram", "quadrant_scatter"] else None,
        "chart": chart,
        "summary": summary,
        "query_type": query_type if query_type != "chart_change" else st.session_state.last_result.get("query_type", "top_videos") if st.session_state.last_result else "top_videos",
        "chart_type": chart_type
    }

    # Store result for future chart type changes
    st.session_state.last_result = {
        "dataframe": df,
        "query_type": response["query_type"],
        "chart_type": chart_type,
        "summary": summary
    }

    st.session_state.demo_messages.append(response)
    st.session_state.last_query_type = response["query_type"]
    st.rerun()
