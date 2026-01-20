"""
Configuration settings for the Video Analytics Chatbot.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# PATHS
# =============================================================================

# Base directory (chatbot folder)
BASE_DIR = Path(__file__).parent

# Parquet data directory (relative to UnifiedPipeline)
DATA_DIR = BASE_DIR.parent / "UnifiedPipeline" / "output" / "parquet"
FACTS_DIR = DATA_DIR / "facts"
DIMENSIONS_DIR = DATA_DIR / "dimensions"

# =============================================================================
# LLM CONFIGURATION
# =============================================================================

# Supported LLM providers
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic")  # "anthropic" or "openai"

# API Keys (loaded from environment or .env file)
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Model settings
ANTHROPIC_MODEL = "claude-sonnet-4-20250514"
OPENAI_MODEL = "gpt-4o-mini"

# =============================================================================
# DATABASE SCHEMA INFORMATION
# =============================================================================

# Schema description for LLM context
SCHEMA_DESCRIPTION = """
DATABASE SCHEMA - Video Analytics Star Schema

TABLE: facts (daily_analytics_all.parquet)
- video_id: STRING - Unique video identifier (join key to dimensions)
- date: DATE - Analytics date
- year: INTEGER - Year extracted from date
- video_view: INTEGER - Total video views
- views_desktop: INTEGER - Views from desktop devices
- views_mobile: INTEGER - Views from mobile devices
- views_tablet: INTEGER - Views from tablets
- views_other: INTEGER - Views from other devices
- video_impression: INTEGER - Number of impressions
- play_rate: FLOAT - Play rate (views/impressions)
- engagement_score: FLOAT - Overall engagement score
- video_engagement_1: FLOAT - % viewers at 1% of video
- video_engagement_25: FLOAT - % viewers at 25% of video
- video_engagement_50: FLOAT - % viewers at 50% of video
- video_engagement_75: FLOAT - % viewers at 75% of video
- video_engagement_100: FLOAT - % viewers who completed video
- video_percent_viewed: FLOAT - Average percent viewed
- video_seconds_viewed: INTEGER - Total seconds viewed
- original_filename: STRING - Original filename
- dt_last_viewed: DATE - Last viewed date
- report_generated_on: DATE - Report generation date

TABLE: dimensions (video_metadata.parquet)
- video_id: STRING - Unique video identifier (primary key)
- account_id: STRING - Account identifier
- channel: STRING - Channel/account name
- name: STRING - Video title/name
- video_duration: INTEGER - Duration in milliseconds
- video_duration_seconds: FLOAT - Duration in seconds
- created_at: DATETIME - Video creation timestamp
- published_at: DATETIME - Video publish timestamp
- original_filename: STRING - Original filename
- created_by: STRING - Creator
- video_content_type: STRING - Content type classification
- video_length: STRING - Length category (short/medium/long)
- video_category: STRING - Video category
- country: STRING - Country
- language: STRING - Language
- business_unit: STRING - Business unit
- tags: STRING - Comma-separated tags
- reference_id: STRING - External reference ID

RELATIONSHIPS:
- facts.video_id -> dimensions.video_id (many-to-one)
- Always JOIN facts with dimensions to get video metadata

IMPORTANT NOTES:
- Use DuckDB SQL syntax
- For aggregations involving video metadata (duration, name), always JOIN with dimensions
- Date filtering: WHERE date >= '2024-01-01'
- Device breakdown: views_desktop, views_mobile, views_tablet, views_other
- Engagement funnel: video_engagement_1 -> video_engagement_25 -> video_engagement_50 -> video_engagement_75 -> video_engagement_100
"""

# =============================================================================
# UI CONFIGURATION
# =============================================================================

# Corporate color scheme - Clean, minimal design
# Red used ONLY for key accents, not backgrounds
COLORS = {
    # Primary palette - minimal red
    "accent": "#E60100",        # Corporate red - use sparingly
    "accent_light": "#FFF0F0",  # Very light red for subtle highlights

    # Neutral palette - dominant
    "black": "#000000",         # Primary text, headings
    "dark": "#1A1A1A",          # Secondary text
    "gray_dark": "#4A4A4A",     # Body text
    "gray": "#6B6B6B",          # Secondary info
    "gray_light": "#9B9B9B",    # Tertiary info
    "border": "#E5E5E5",        # Borders and dividers
    "surface": "#F7F7F7",       # Card backgrounds
    "background": "#FFFFFF",    # Page background

    # Semantic colors
    "success": "#00875A",       # Positive metrics
    "warning": "#B25000",       # Warnings
    "error": "#D32F2F",         # Errors (different from brand red)

    # Chart colors - professional palette
    "chart_1": "#2D5BFF",       # Primary blue
    "chart_2": "#00B8A9",       # Teal
    "chart_3": "#7C4DFF",       # Purple
    "chart_4": "#FF6B6B",       # Coral (not brand red)
    "chart_5": "#FFB800",       # Amber
}

APP_TITLE = "Video Analytics Intelligence"
APP_ICON = "◆"  # Diamond - clean, professional
