"""
LLM integration module for text-to-SQL conversion.
"""
import re
from typing import Optional, Tuple
from abc import ABC, abstractmethod

from config import (
    LLM_PROVIDER,
    ANTHROPIC_API_KEY,
    OPENAI_API_KEY,
    ANTHROPIC_MODEL,
    OPENAI_MODEL,
    SCHEMA_DESCRIPTION,
)


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    def generate_sql(self, question: str, schema: str) -> Tuple[str, Optional[str]]:
        """
        Generate SQL from natural language question.

        Returns:
            Tuple of (SQL query, error message or None)
        """
        pass

    @abstractmethod
    def summarize_results(self, question: str, sql: str, results: str) -> str:
        """
        Generate natural language summary of query results.
        """
        pass

    @abstractmethod
    def is_configured(self) -> bool:
        """Check if the provider is properly configured."""
        pass


class AnthropicProvider(LLMProvider):
    """Claude API provider."""

    def __init__(self):
        self.api_key = ANTHROPIC_API_KEY
        self.model = ANTHROPIC_MODEL
        self._client = None

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get_client(self):
        if self._client is None:
            import anthropic
            self._client = anthropic.Anthropic(api_key=self.api_key)
        return self._client

    def generate_sql(self, question: str, schema: str) -> Tuple[str, Optional[str]]:
        if not self.is_configured():
            return "", "Anthropic API key not configured"

        try:
            client = self._get_client()

            system_prompt = f"""You are a SQL expert assistant. Convert natural language questions to DuckDB SQL queries.

{SCHEMA_DESCRIPTION}

LIVE SCHEMA FROM DATABASE:
{schema}

RULES:
1. Return ONLY the SQL query, no explanations or markdown
2. Use DuckDB SQL syntax
3. Always limit results to 50 rows unless user specifies otherwise
4. For video metadata (name, duration, channel), JOIN facts with dimensions on video_id
5. Use appropriate aggregations (SUM, AVG, COUNT) based on the question
6. Format dates as 'YYYY-MM-DD'
7. Order results meaningfully (usually by the main metric descending)
"""

            response = client.messages.create(
                model=self.model,
                max_tokens=1000,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": f"Question: {question}"}
                ]
            )

            sql = response.content[0].text.strip()
            sql = self._clean_sql(sql)
            return sql, None

        except Exception as e:
            return "", str(e)

    def summarize_results(self, question: str, sql: str, results: str) -> str:
        if not self.is_configured():
            return "API not configured for summarization."

        try:
            client = self._get_client()

            response = client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[
                    {
                        "role": "user",
                        "content": f"""Based on this question and query results, provide a brief, helpful summary.

Question: {question}

SQL Query:
{sql}

Results:
{results}

Provide a concise 2-3 sentence summary highlighting the key findings. Use specific numbers from the results."""
                    }
                ]
            )

            return response.content[0].text.strip()

        except Exception as e:
            return f"Could not generate summary: {e}"

    def _clean_sql(self, sql: str) -> str:
        """Remove markdown code blocks and clean up SQL."""
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)
        sql = sql.strip()
        return sql


class OpenAIProvider(LLMProvider):
    """OpenAI API provider."""

    def __init__(self):
        self.api_key = OPENAI_API_KEY
        self.model = OPENAI_MODEL
        self._client = None

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key)
        return self._client

    def generate_sql(self, question: str, schema: str) -> Tuple[str, Optional[str]]:
        if not self.is_configured():
            return "", "OpenAI API key not configured"

        try:
            client = self._get_client()

            system_prompt = f"""You are a SQL expert assistant. Convert natural language questions to DuckDB SQL queries.

{SCHEMA_DESCRIPTION}

LIVE SCHEMA FROM DATABASE:
{schema}

RULES:
1. Return ONLY the SQL query, no explanations or markdown
2. Use DuckDB SQL syntax
3. Always limit results to 50 rows unless user specifies otherwise
4. For video metadata (name, duration, channel), JOIN facts with dimensions on video_id
5. Use appropriate aggregations (SUM, AVG, COUNT) based on the question
6. Format dates as 'YYYY-MM-DD'
7. Order results meaningfully (usually by the main metric descending)
"""

            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Question: {question}"}
                ],
                max_tokens=1000
            )

            sql = response.choices[0].message.content.strip()
            sql = self._clean_sql(sql)
            return sql, None

        except Exception as e:
            return "", str(e)

    def summarize_results(self, question: str, sql: str, results: str) -> str:
        if not self.is_configured():
            return "API not configured for summarization."

        try:
            client = self._get_client()

            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": f"""Based on this question and query results, provide a brief, helpful summary.

Question: {question}

SQL Query:
{sql}

Results:
{results}

Provide a concise 2-3 sentence summary highlighting the key findings. Use specific numbers from the results."""
                    }
                ],
                max_tokens=500
            )

            return response.choices[0].message.content.strip()

        except Exception as e:
            return f"Could not generate summary: {e}"

    def _clean_sql(self, sql: str) -> str:
        """Remove markdown code blocks and clean up SQL."""
        sql = re.sub(r'```sql\s*', '', sql)
        sql = re.sub(r'```\s*', '', sql)
        sql = sql.strip()
        return sql


def get_llm_provider() -> LLMProvider:
    """
    Get the configured LLM provider.
    """
    if LLM_PROVIDER.lower() == "openai":
        return OpenAIProvider()
    else:
        return AnthropicProvider()


def check_llm_status() -> Tuple[bool, str]:
    """
    Check if LLM is properly configured.

    Returns:
        Tuple of (is_configured, status_message)
    """
    provider = get_llm_provider()

    if provider.is_configured():
        provider_name = "Anthropic Claude" if isinstance(provider, AnthropicProvider) else "OpenAI"
        return True, f"{provider_name} configured"
    else:
        return False, f"API key not set. Add to .env file:\n" + \
               f"ANTHROPIC_API_KEY=your-key or OPENAI_API_KEY=your-key"
