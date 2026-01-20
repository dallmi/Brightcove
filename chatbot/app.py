"""
Video Analytics Chatbot - Streamlit Application

A natural language interface for querying video analytics data.
"""
import streamlit as st
import pandas as pd
from datetime import datetime

from config import APP_TITLE, APP_ICON, COLORS
from database import get_database
from llm import get_llm_provider, check_llm_status

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
# CUSTOM CSS - Corporate Design
# =============================================================================

st.markdown(f"""
<style>
    /* Import professional font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global styles */
    html, body, [class*="css"] {{
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}

    /* Main container */
    .main .block-container {{
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }}

    /* Header styling */
    .main-header {{
        background: linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['primary_dark']} 100%);
        color: white;
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }}

    .main-header h1 {{
        margin: 0;
        font-size: 1.75rem;
        font-weight: 600;
        letter-spacing: -0.02em;
    }}

    .main-header p {{
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
        font-size: 0.95rem;
    }}

    /* Chat container */
    .chat-container {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }}

    /* Message styling */
    .user-message {{
        background: {COLORS['surface']};
        border-left: 4px solid {COLORS['primary']};
        padding: 1rem 1.25rem;
        border-radius: 0 8px 8px 0;
        margin: 1rem 0;
    }}

    .assistant-message {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 1rem 0;
    }}

    /* SQL code block */
    .sql-block {{
        background: #1e1e1e;
        color: #d4d4d4;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
        font-size: 0.85rem;
        overflow-x: auto;
        margin: 0.75rem 0;
    }}

    /* Results table */
    .dataframe {{
        font-size: 0.85rem !important;
    }}

    .dataframe th {{
        background: {COLORS['surface']} !important;
        color: {COLORS['secondary']} !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.75rem !important;
        letter-spacing: 0.05em;
    }}

    /* Status indicators */
    .status-badge {{
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
    }}

    .status-success {{
        background: #E6F4EA;
        color: {COLORS['success']};
    }}

    .status-error {{
        background: #FEECED;
        color: {COLORS['error']};
    }}

    .status-warning {{
        background: #FFF8E6;
        color: #B25000;
    }}

    /* Sidebar styling */
    [data-testid="stSidebar"] {{
        background: {COLORS['surface']};
    }}

    [data-testid="stSidebar"] .block-container {{
        padding-top: 2rem;
    }}

    /* Button styling */
    .stButton > button {{
        background: {COLORS['primary']};
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 500;
        transition: all 0.2s ease;
    }}

    .stButton > button:hover {{
        background: {COLORS['primary_dark']};
        box-shadow: 0 2px 8px rgba(230, 0, 0, 0.3);
    }}

    /* Input styling */
    .stTextInput > div > div > input {{
        border-radius: 8px;
        border: 1px solid {COLORS['border']};
        padding: 0.75rem 1rem;
    }}

    .stTextInput > div > div > input:focus {{
        border-color: {COLORS['primary']};
        box-shadow: 0 0 0 2px rgba(230, 0, 0, 0.1);
    }}

    /* Metric cards */
    .metric-card {{
        background: {COLORS['background']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }}

    .metric-value {{
        font-size: 1.5rem;
        font-weight: 600;
        color: {COLORS['primary']};
    }}

    .metric-label {{
        font-size: 0.8rem;
        color: {COLORS['text_light']};
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    /* Example queries */
    .example-query {{
        background: {COLORS['surface']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        cursor: pointer;
        transition: all 0.2s ease;
        font-size: 0.9rem;
    }}

    .example-query:hover {{
        border-color: {COLORS['primary']};
        background: white;
    }}

    /* Hide Streamlit branding */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}

    /* Expander styling */
    .streamlit-expanderHeader {{
        font-weight: 500;
        color: {COLORS['secondary']};
    }}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================

if "messages" not in st.session_state:
    st.session_state.messages = []

if "db" not in st.session_state:
    st.session_state.db = get_database()

if "llm" not in st.session_state:
    st.session_state.llm = get_llm_provider()

# =============================================================================
# SIDEBAR
# =============================================================================

with st.sidebar:
    st.markdown("### Configuration")

    # Data status
    db = st.session_state.db
    data_available, data_message = db.check_data_available()

    st.markdown("**Data Connection**")
    if data_available:
        st.markdown(f'<span class="status-badge status-success">Connected</span>', unsafe_allow_html=True)
        stats = db.get_table_stats()
        if "facts" in stats:
            st.caption(f"Facts: {stats['facts'].get('row_count', 0):,} rows")
        if "dimensions" in stats:
            st.caption(f"Videos: {stats['dimensions'].get('row_count', 0):,} unique")
    else:
        st.markdown(f'<span class="status-badge status-error">Not Connected</span>', unsafe_allow_html=True)
        st.caption(data_message)

    st.markdown("---")

    # LLM status
    llm_configured, llm_message = check_llm_status()

    st.markdown("**AI Assistant**")
    if llm_configured:
        st.markdown(f'<span class="status-badge status-success">{llm_message}</span>', unsafe_allow_html=True)
    else:
        st.markdown(f'<span class="status-badge status-warning">Not Configured</span>', unsafe_allow_html=True)
        st.caption(llm_message)

    st.markdown("---")

    # Example queries
    st.markdown("**Example Questions**")

    example_queries = [
        "What are the top 10 videos by total views?",
        "Show views by device type",
        "Which channels have the most videos?",
        "What's the average engagement score by channel?",
        "Show me videos with completion rate above 50%",
    ]

    for query in example_queries:
        if st.button(query, key=f"example_{hash(query)}", use_container_width=True):
            st.session_state.pending_query = query
            st.rerun()

    st.markdown("---")

    # Clear chat
    if st.button("Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    # Schema info
    with st.expander("View Schema"):
        if data_available:
            st.code(db.get_schema_string(), language="text")
        else:
            st.caption("Load data to view schema")

# =============================================================================
# MAIN CONTENT
# =============================================================================

# Header
st.markdown(f"""
<div class="main-header">
    <h1>{APP_ICON} {APP_TITLE}</h1>
    <p>Ask questions about your video analytics in plain English</p>
</div>
""", unsafe_allow_html=True)

# Check prerequisites
if not data_available:
    st.warning(f"""
    **Data Not Available**

    Please ensure the Parquet files are generated by running the UnifiedPipeline.

    Expected files:
    - `UnifiedPipeline/output/parquet/facts/daily_analytics_all.parquet`
    - `UnifiedPipeline/output/parquet/dimensions/video_metadata.parquet`
    """)

if not llm_configured:
    st.info(f"""
    **API Key Required**

    Create a `.env` file in the chatbot folder with your API key:

    ```
    ANTHROPIC_API_KEY=your-key-here
    ```

    Or for OpenAI:
    ```
    OPENAI_API_KEY=your-key-here
    LLM_PROVIDER=openai
    ```
    """)

# Display chat history
for message in st.session_state.messages:
    if message["role"] == "user":
        st.markdown(f"""
        <div class="user-message">
            <strong>You:</strong> {message["content"]}
        </div>
        """, unsafe_allow_html=True)
    else:
        with st.container():
            st.markdown(f"""
            <div class="assistant-message">
            """, unsafe_allow_html=True)

            if "sql" in message:
                st.markdown("**Generated SQL:**")
                st.code(message["sql"], language="sql")

            if "dataframe" in message and message["dataframe"] is not None:
                st.markdown("**Results:**")
                st.dataframe(message["dataframe"], use_container_width=True)

            if "summary" in message:
                st.markdown("**Summary:**")
                st.markdown(message["summary"])

            if "error" in message:
                st.error(message["error"])

            st.markdown("</div>", unsafe_allow_html=True)

# Handle pending query from sidebar
if "pending_query" in st.session_state:
    pending = st.session_state.pending_query
    del st.session_state.pending_query

    # Process the query
    if data_available and llm_configured:
        st.session_state.messages.append({"role": "user", "content": pending})

        # Generate SQL
        llm = st.session_state.llm
        schema = db.get_schema_string()
        sql, sql_error = llm.generate_sql(pending, schema)

        response = {"role": "assistant"}

        if sql_error:
            response["error"] = f"Failed to generate SQL: {sql_error}"
        else:
            response["sql"] = sql

            # Execute query
            df, exec_error = db.execute_query(sql)

            if exec_error:
                response["error"] = f"Query execution failed: {exec_error}"
            else:
                response["dataframe"] = df

                # Generate summary
                if df is not None and len(df) > 0:
                    results_str = df.head(20).to_string()
                    summary = llm.summarize_results(pending, sql, results_str)
                    response["summary"] = summary

        st.session_state.messages.append(response)
        st.rerun()

# Chat input
if prompt := st.chat_input("Ask a question about your video analytics..."):
    if not data_available:
        st.error("Please load data first.")
    elif not llm_configured:
        st.error("Please configure your API key first.")
    else:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})

        # Generate SQL
        llm = st.session_state.llm
        schema = db.get_schema_string()

        with st.spinner("Analyzing your question..."):
            sql, sql_error = llm.generate_sql(prompt, schema)

        response = {"role": "assistant"}

        if sql_error:
            response["error"] = f"Failed to generate SQL: {sql_error}"
        else:
            response["sql"] = sql

            # Execute query
            with st.spinner("Running query..."):
                df, exec_error = db.execute_query(sql)

            if exec_error:
                response["error"] = f"Query execution failed: {exec_error}"
            else:
                response["dataframe"] = df

                # Generate summary
                if df is not None and len(df) > 0:
                    with st.spinner("Generating summary..."):
                        results_str = df.head(20).to_string()
                        summary = llm.summarize_results(prompt, sql, results_str)
                        response["summary"] = summary

        st.session_state.messages.append(response)
        st.rerun()

# Footer with stats
if data_available:
    stats = db.get_table_stats()
    if stats:
        st.markdown("---")
        cols = st.columns(4)

        if "facts" in stats:
            with cols[0]:
                st.metric("Total Records", f"{stats['facts'].get('row_count', 0):,}")
            with cols[1]:
                total_views = stats['facts'].get('total_views', 0)
                if total_views:
                    st.metric("Total Views", f"{total_views:,}")
            with cols[2]:
                date_range = stats['facts'].get('date_range', {})
                if date_range.get('min') and date_range.get('max'):
                    st.metric("Date Range", f"{date_range['min'][:10]} to {date_range['max'][:10]}")

        if "dimensions" in stats:
            with cols[3]:
                st.metric("Unique Videos", f"{stats['dimensions'].get('row_count', 0):,}")
