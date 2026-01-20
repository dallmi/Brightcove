"""
Database module for connecting to Parquet files via DuckDB.
"""
import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from config import FACTS_DIR, DIMENSIONS_DIR, DATA_DIR


class VideoDatabase:
    """
    Manages DuckDB connection to Parquet files.
    """

    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self.tables_loaded = False
        self._setup_views()

    def _setup_views(self) -> bool:
        """
        Create views for the Parquet files if they exist.
        Returns True if at least one table was loaded.
        """
        tables_created = []

        # Facts table - daily analytics
        facts_path = FACTS_DIR / "daily_analytics_all.parquet"
        if facts_path.exists():
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW facts AS
                SELECT * FROM read_parquet('{facts_path}')
            """)
            tables_created.append("facts")

        # Dimensions table - video metadata
        dimensions_path = DIMENSIONS_DIR / "video_metadata.parquet"
        if dimensions_path.exists():
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW dimensions AS
                SELECT * FROM read_parquet('{dimensions_path}')
            """)
            tables_created.append("dimensions")

        self.tables_loaded = len(tables_created) > 0
        return self.tables_loaded

    def get_schema_info(self) -> Dict[str, List[Dict[str, str]]]:
        """
        Get schema information for all loaded tables.
        """
        schema = {}

        for table in ["facts", "dimensions"]:
            try:
                result = self.conn.execute(f"DESCRIBE {table}").fetchall()
                schema[table] = [
                    {"column": row[0], "type": row[1]}
                    for row in result
                ]
            except Exception:
                pass

        return schema

    def get_schema_string(self) -> str:
        """
        Get schema as formatted string for LLM context.
        """
        schema = self.get_schema_info()

        if not schema:
            return "No tables loaded. Please ensure Parquet files exist."

        lines = []
        for table, columns in schema.items():
            lines.append(f"\nTABLE: {table}")
            lines.append("-" * 40)
            for col in columns:
                lines.append(f"  {col['column']}: {col['type']}")

        return "\n".join(lines)

    def execute_query(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Execute a SQL query and return results or error.

        Returns:
            Tuple of (DataFrame or None, error message or None)
        """
        try:
            # Basic safety check - only allow SELECT statements
            sql_upper = sql.strip().upper()
            if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
                return None, "Only SELECT queries are allowed."

            # Check for dangerous operations
            dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
            for word in dangerous:
                if word in sql_upper:
                    return None, f"Operation '{word}' is not allowed."

            # Execute query with timeout
            result = self.conn.execute(sql).fetchdf()
            return result, None

        except Exception as e:
            return None, str(e)

    def get_sample_data(self, table: str, limit: int = 5) -> Optional[pd.DataFrame]:
        """
        Get sample rows from a table.
        """
        try:
            return self.conn.execute(f"SELECT * FROM {table} LIMIT {limit}").fetchdf()
        except Exception:
            return None

    def get_table_stats(self) -> Dict[str, Any]:
        """
        Get basic statistics about loaded tables.
        """
        stats = {}

        for table in ["facts", "dimensions"]:
            try:
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[table] = {"row_count": count}

                if table == "facts":
                    # Get date range
                    date_range = self.conn.execute("""
                        SELECT MIN(date), MAX(date) FROM facts
                    """).fetchone()
                    stats[table]["date_range"] = {
                        "min": str(date_range[0]) if date_range[0] else None,
                        "max": str(date_range[1]) if date_range[1] else None
                    }

                    # Get total views
                    total_views = self.conn.execute("""
                        SELECT SUM(video_view) FROM facts
                    """).fetchone()[0]
                    stats[table]["total_views"] = total_views

                elif table == "dimensions":
                    # Get unique channels
                    channels = self.conn.execute("""
                        SELECT COUNT(DISTINCT channel) FROM dimensions
                    """).fetchone()[0]
                    stats[table]["unique_channels"] = channels

            except Exception:
                pass

        return stats

    def check_data_available(self) -> Tuple[bool, str]:
        """
        Check if data is available and return status message.
        """
        if not self.tables_loaded:
            # Check if parquet directory exists
            if not DATA_DIR.exists():
                return False, f"Data directory not found: {DATA_DIR}"

            facts_path = FACTS_DIR / "daily_analytics_all.parquet"
            dimensions_path = DIMENSIONS_DIR / "video_metadata.parquet"

            missing = []
            if not facts_path.exists():
                missing.append(f"facts: {facts_path}")
            if not dimensions_path.exists():
                missing.append(f"dimensions: {dimensions_path}")

            if missing:
                return False, f"Missing Parquet files:\n" + "\n".join(missing)

            return False, "Unknown error loading tables"

        return True, "Data loaded successfully"

    def close(self):
        """Close the database connection."""
        self.conn.close()


# Singleton instance
_db_instance: Optional[VideoDatabase] = None


def get_database() -> VideoDatabase:
    """Get or create the database singleton."""
    global _db_instance
    if _db_instance is None:
        _db_instance = VideoDatabase()
    return _db_instance
