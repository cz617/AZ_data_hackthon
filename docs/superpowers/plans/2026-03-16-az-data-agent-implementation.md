# AZ Data Agent Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an AI-powered data analysis system with monitoring, alerting, and intelligent agent capabilities for AstraZeneca pharmaceutical data.

**Architecture:** Modular Python application with four main components: Core (shared services), Monitor (scheduled SQL metrics), Agent (LangChain-based AI), and Web UI (Streamlit). Components communicate via SQLite-based message queue.

**Tech Stack:** Python 3.10+, LangChain, Snowflake Connector, Streamlit, APScheduler, SQLite, Pydantic

---

## File Structure

```
az-data-agent/
├── src/
│   ├── core/                    # Shared services
│   │   ├── __init__.py
│   │   ├── config.py           # Pydantic Settings
│   │   ├── database.py         # Snowflake connection pool
│   │   └── llm_provider.py     # LLM abstraction layer
│   │
│   ├── monitor/                 # Monitoring service
│   │   ├── __init__.py
│   │   ├── models.py           # SQLAlchemy models
│   │   ├── metrics_loader.py   # YAML metrics loader
│   │   ├── executor.py         # SQL executor
│   │   ├── alert_engine.py     # Threshold checker
│   │   └── scheduler.py        # APScheduler service
│   │
│   ├── agent/                   # AI Agent
│   │   ├── __init__.py
│   │   ├── agent.py            # Main agent factory
│   │   ├── tools/
│   │   │   ├── __init__.py
│   │   │   ├── snowflake_tool.py
│   │   │   └── chart_tool.py
│   │   ├── skills/
│   │   │   ├── __init__.py
│   │   │   ├── base.py         # Skill base class
│   │   │   └── sql_analyzer.py
│   │   └── middleware/
│   │       ├── __init__.py
│   │       └── context_enricher.py
│   │
│   ├── web/                     # Streamlit UI
│   │   ├── __init__.py
│   │   ├── app.py              # Main entry
│   │   └── components/
│   │       ├── __init__.py
│   │       └── chat.py
│   │
│   └── messaging/
│       ├── __init__.py
│       └── queue.py            # SQLite queue
│
├── config/
│   ├── settings.yaml
│   ├── metrics_template.yaml
│   └── prompts/
│       └── system_prompt.md
│
├── tests/
│   ├── conftest.py
│   ├── test_core/
│   ├── test_monitor/
│   └── test_agent/
│
├── scripts/
│   ├── init_db.py
│   └── start_all.sh
│
├── data/                        # SQLite database files
│   └── .gitkeep
│
├── pyproject.toml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Chunk 1: Project Setup & Core Module

### Task 1: Initialize Project Structure

**Files:**
- Create: `pyproject.toml`
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `.gitignore`
- Create: `README.md`
- Create: `src/__init__.py`
- Create: `src/core/__init__.py`
- Create: `data/.gitkeep`

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "az-data-agent"
version = "0.1.0"
description = "AI Agent for AstraZeneca data analysis with monitoring capabilities"
requires-python = ">=3.10"
dependencies = [
    "langchain>=0.3.0",
    "langchain-anthropic>=0.3.0",
    "langchain-openai>=0.3.0",
    "snowflake-connector-python>=3.0.0",
    "streamlit>=1.30.0",
    "apscheduler>=3.10.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    "sqlalchemy>=2.0.0",
    "pyyaml>=6.0",
    "plotly>=5.0.0",
    "pandas>=2.0.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.0.0",
    "black>=24.0.0",
    "isort>=5.0.0",
    "mypy>=1.0.0",
]

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[tool.black]
line-length = 88
target-version = ["py310"]

[tool.isort]
profile = "black"
line_length = 88

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"
```

- [ ] **Step 2: Create requirements.txt**

```txt
langchain>=0.3.0
langchain-anthropic>=0.3.0
langchain-openai>=0.3.0
snowflake-connector-python>=3.0.0
streamlit>=1.30.0
apscheduler>=3.10.0
pydantic>=2.0.0
pydantic-settings>=2.0.0
sqlalchemy>=2.0.0
pyyaml>=6.0
plotly>=5.0.0
pandas>=2.0.0
python-dotenv>=1.0.0
```

- [ ] **Step 3: Create .env.example**

```bash
# LLM Configuration
LLM_PROVIDER=claude
LLM_MODEL=claude-sonnet-4-5-20250929
LLM_API_KEY=your-api-key-here

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ENT_HACKATHON_DATA_SHARE
SNOWFLAKE_SCHEMA=EA_HACKATHON

# Monitor Configuration
MONITOR_INTERVAL_MINUTES=5
```

- [ ] **Step 4: Create .gitignore**

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
.env
.venv
env/
venv/
ENV/

# IDE
.idea/
.vscode/
*.swp
*.swo

# Data
data/*.db
data/*.sqlite
*.db
*.sqlite

# Streamlit
.streamlit/

# Logs
*.log
logs/

# OS
.DS_Store
Thumbs.db
```

- [ ] **Step 5: Create README.md**

```markdown
# AZ Data Agent

AI-powered data analysis system for AstraZeneca pharmaceutical data.

## Features

- **Intelligent Monitoring**: Scheduled SQL metric execution with threshold-based alerting
- **AI Analysis**: LangChain-powered agent for natural language data queries
- **Auto-Alert Analysis**: Automatic deep-dive analysis when thresholds are breached
- **Web Interface**: Streamlit-based chat interface

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials

# Initialize database
python scripts/init_db.py

# Start services
./scripts/start_all.sh
```

## Architecture

See [Design Document](docs/superpowers/specs/2026-03-16-az-data-agent-design.md)

## License

MIT
```

- [ ] **Step 6: Create __init__.py files**

Create empty `__init__.py` files in:
- `src/__init__.py`
- `src/core/__init__.py`

- [ ] **Step 7: Create data directory placeholder**

Create `data/.gitkeep` (empty file)

- [ ] **Step 8: Commit project setup**

```bash
git add pyproject.toml requirements.txt .env.example .gitignore README.md src/ data/.gitkeep
git commit -m "chore: Initialize project structure

- Add pyproject.toml with dependencies
- Add requirements.txt
- Add .env.example template
- Add .gitignore
- Add README.md
- Create src/ and data/ directories"
```

---

### Task 2: Configuration Management

**Files:**
- Create: `src/core/config.py`
- Create: `tests/conftest.py`
- Create: `tests/test_core/__init__.py`
- Create: `tests/test_core/test_config.py`

- [ ] **Step 1: Write failing test for Settings**

```python
# tests/test_core/test_config.py
"""Tests for configuration management."""
import os
import pytest
from pathlib import Path


def test_settings_loads_from_env(monkeypatch):
    """Test that Settings loads values from environment variables."""
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-4")
    monkeypatch.setenv("LLM_API_KEY", "test-key")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
    monkeypatch.setenv("MONITOR_INTERVAL_MINUTES", "10")

    from src.core.config import Settings

    settings = Settings()

    assert settings.llm_provider == "openai"
    assert settings.llm_model == "gpt-4"
    assert settings.llm_api_key == "test-key"
    assert settings.snowflake_account == "test-account"
    assert settings.monitor_interval_minutes == 10


def test_settings_has_defaults():
    """Test that Settings has sensible defaults."""
    from src.core.config import Settings

    # Create with minimal env (clear any existing)
    settings = Settings()

    assert settings.llm_provider == "claude"
    assert settings.snowflake_database == "ENT_HACKATHON_DATA_SHARE"
    assert settings.snowflake_schema == "EA_HACKATHON"
    assert settings.monitor_interval_minutes == 5


def test_settings_validates_llm_provider():
    """Test that Settings validates LLM provider."""
    import pydantic
    from src.core.config import Settings

    with pytest.raises(pydantic.ValidationError):
        Settings(llm_provider="invalid_provider")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_core/test_config.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.core.config'"

- [ ] **Step 3: Create conftest.py**

```python
# tests/conftest.py
"""Pytest configuration and fixtures."""
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))
```

- [ ] **Step 4: Implement Settings class**

```python
# src/core/config.py
"""Configuration management using Pydantic Settings."""
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # LLM Configuration
    llm_provider: Literal["claude", "openai", "azure"] = "claude"
    llm_model: str = "claude-sonnet-4-5-20250929"
    llm_api_key: str = ""

    # Azure-specific (only needed if llm_provider == "azure")
    azure_openai_endpoint: str = ""
    azure_openai_deployment: str = ""
    azure_openai_api_version: str = "2024-02-01"

    # Snowflake Configuration
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "ENT_HACKATHON_DATA_SHARE"
    snowflake_schema: str = "EA_HACKATHON"
    snowflake_role: str = ""

    # Monitor Configuration
    monitor_interval_minutes: int = 5
    monitor_enabled: bool = True

    # Application Configuration
    app_name: str = "AZ Data Agent"
    debug: bool = False
    log_level: str = "INFO"


# Global settings instance
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset the global settings instance (useful for testing)."""
    global _settings
    _settings = None
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_core/test_config.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit configuration module**

```bash
git add src/core/config.py tests/conftest.py tests/test_core/
git commit -m "feat(core): Add configuration management with Pydantic Settings

- Add Settings class with LLM, Snowflake, and monitor config
- Add environment variable loading
- Add tests for configuration loading"
```

---

### Task 3: Database Connection Module

**Files:**
- Create: `src/core/database.py`
- Create: `tests/test_core/test_database.py`

- [ ] **Step 1: Write failing test for database connection**

```python
# tests/test_core/test_database.py
"""Tests for database connection module."""
import pytest
from unittest.mock import patch, MagicMock


def test_get_snowflake_connection_returns_connection():
    """Test that get_snowflake_connection returns a connection object."""
    with patch("src.core.database.snowflake.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        from src.core.database import get_snowflake_connection
        from src.core.config import Settings

        settings = Settings(
            snowflake_account="test",
            snowflake_user="user",
            snowflake_password="pass",
        )
        conn = get_snowflake_connection(settings)

        assert conn == mock_conn
        mock_connect.assert_called_once()


def test_execute_query_returns_results():
    """Test that execute_query returns query results."""
    with patch("src.core.database.get_snowflake_connection") as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]
        mock_cursor.description = [("id",), ("name",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        from src.core.database import execute_query
        from src.core.config import Settings

        settings = Settings()
        results = execute_query("SELECT * FROM test", settings)

        assert len(results) == 2
        assert results[0] == (1, "test")


def test_execute_query_handles_errors():
    """Test that execute_query properly handles and re-raises errors."""
    with patch("src.core.database.get_snowflake_connection") as mock_get_conn:
        mock_get_conn.side_effect = Exception("Connection failed")

        from src.core.database import execute_query
        from src.core.config import Settings

        settings = Settings()

        with pytest.raises(Exception, match="Connection failed"):
            execute_query("SELECT 1", settings)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_core/test_database.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.core.database'"

- [ ] **Step 3: Implement database module**

```python
# src/core/database.py
"""Snowflake database connection and query execution."""
from typing import Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.core.config import Settings


def get_snowflake_connection(settings: Settings) -> SnowflakeConnection:
    """
    Create and return a Snowflake connection.

    Args:
        settings: Application settings containing connection parameters

    Returns:
        SnowflakeConnection: Active Snowflake connection
    """
    connection_params = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "password": settings.snowflake_password,
        "warehouse": settings.snowflake_warehouse,
        "database": settings.snowflake_database,
        "schema": settings.snowflake_schema,
    }

    # Add role if specified
    if settings.snowflake_role:
        connection_params["role"] = settings.snowflake_role

    return snowflake.connector.connect(**connection_params)


def execute_query(
    sql: str,
    settings: Settings,
    params: dict[str, Any] | None = None,
) -> list[tuple]:
    """
    Execute a SQL query and return results.

    Args:
        sql: SQL query to execute
        settings: Application settings for database connection
        params: Optional query parameters

    Returns:
        List of result tuples

    Raises:
        Exception: If query execution fails
    """
    conn = get_snowflake_connection(settings)

    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()


def execute_query_with_columns(
    sql: str,
    settings: Settings,
    params: dict[str, Any] | None = None,
) -> tuple[list[str], list[tuple]]:
    """
    Execute a SQL query and return column names and results.

    Args:
        sql: SQL query to execute
        settings: Application settings for database connection
        params: Optional query parameters

    Returns:
        Tuple of (column_names, result_rows)
    """
    conn = get_snowflake_connection(settings)

    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            return columns, results
    finally:
        conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_core/test_database.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit database module**

```bash
git add src/core/database.py tests/test_core/test_database.py
git commit -m "feat(core): Add Snowflake database connection module

- Add get_snowflake_connection function
- Add execute_query and execute_query_with_columns functions
- Add tests for database operations"
```

---

### Task 4: LLM Provider Module

**Files:**
- Create: `src/core/llm_provider.py`
- Create: `tests/test_core/test_llm_provider.py`

- [ ] **Step 1: Write failing test for LLM provider**

```python
# tests/test_core/test_llm_provider.py
"""Tests for LLM provider module."""
import pytest
from unittest.mock import patch, MagicMock


def test_get_llm_returns_claude_model():
    """Test that get_llm returns Claude model when provider is claude."""
    with patch("src.core.llm_provider.ChatAnthropic") as mock_claude:
        mock_model = MagicMock()
        mock_claude.return_value = mock_model

        from src.core.llm_provider import get_llm
        from src.core.config import Settings

        settings = Settings(
            llm_provider="claude",
            llm_model="claude-sonnet-4-5-20250929",
            llm_api_key="test-key",
        )
        llm = get_llm(settings)

        assert llm == mock_model
        mock_claude.assert_called_once_with(
            model="claude-sonnet-4-5-20250929",
            api_key="test-key",
        )


def test_get_llm_returns_openai_model():
    """Test that get_llm returns OpenAI model when provider is openai."""
    with patch("src.core.llm_provider.ChatOpenAI") as mock_openai:
        mock_model = MagicMock()
        mock_openai.return_value = mock_model

        from src.core.llm_provider import get_llm
        from src.core.config import Settings

        settings = Settings(
            llm_provider="openai",
            llm_model="gpt-4",
            llm_api_key="test-key",
        )
        llm = get_llm(settings)

        assert llm == mock_model
        mock_openai.assert_called_once()


def test_get_llm_raises_for_unsupported_provider():
    """Test that get_llm raises error for unsupported provider."""
    from src.core.llm_provider import get_llm, UnsupportedProviderError
    from src.core.config import Settings

    # Create settings with invalid provider by bypassing validation
    settings = Settings()

    with pytest.raises(UnsupportedProviderError):
        get_llm(settings, provider="unsupported")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_core/test_llm_provider.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'src.core.llm_provider'"

- [ ] **Step 3: Implement LLM provider module**

```python
# src/core/llm_provider.py
"""LLM provider abstraction for multiple model backends."""
from typing import Literal

from langchain_anthropic import ChatAnthropic
from langchain_openai import AzureChatOpenAI, ChatOpenAI

from src.core.config import Settings


class UnsupportedProviderError(Exception):
    """Raised when an unsupported LLM provider is requested."""

    pass


def get_llm(
    settings: Settings,
    provider: Literal["claude", "openai", "azure"] | None = None,
):
    """
    Get an LLM instance based on settings.

    Args:
        settings: Application settings containing LLM configuration
        provider: Optional provider override (defaults to settings.llm_provider)

    Returns:
        LangChain chat model instance

    Raises:
        UnsupportedProviderError: If provider is not supported
    """
    provider = provider or settings.llm_provider

    if provider == "claude":
        return ChatAnthropic(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
        )

    elif provider == "openai":
        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
        )

    elif provider == "azure":
        if not settings.azure_openai_endpoint:
            raise ValueError(
                "Azure OpenAI requires azure_openai_endpoint to be set"
            )

        return AzureChatOpenAI(
            azure_deployment=settings.azure_openai_deployment,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.azure_openai_api_version,
            api_key=settings.llm_api_key,
        )

    else:
        raise UnsupportedProviderError(
            f"Unsupported LLM provider: {provider}. "
            f"Supported providers: claude, openai, azure"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_core/test_llm_provider.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit LLM provider module**

```bash
git add src/core/llm_provider.py tests/test_core/test_llm_provider.py
git commit -m "feat(core): Add LLM provider abstraction layer

- Support for Claude, OpenAI, and Azure OpenAI
- Unified interface via get_llm function
- Add UnsupportedProviderError for invalid providers"
```

---

### Task 5: Core Module Integration Test

**Files:**
- Create: `tests/test_core/test_integration.py`

- [ ] **Step 1: Write integration test for core module**

```python
# tests/test_core/test_integration.py
"""Integration tests for core module."""
import pytest


def test_core_module_exports():
    """Test that core module exports all expected components."""
    from src.core import Settings, get_settings, reset_settings
    from src.core.config import Settings as ConfigSettings
    from src.core.database import get_snowflake_connection, execute_query
    from src.core.llm_provider import get_llm, UnsupportedProviderError

    # Verify exports
    assert Settings is ConfigSettings
    assert callable(get_settings)
    assert callable(reset_settings)
    assert callable(get_snowflake_connection)
    assert callable(execute_query)
    assert callable(get_llm)
    assert issubclass(UnsupportedProviderError, Exception)


def test_settings_singleton():
    """Test that get_settings returns a singleton."""
    from src.core.config import get_settings, reset_settings

    reset_settings()
    settings1 = get_settings()
    settings2 = get_settings()

    assert settings1 is settings2

    reset_settings()
```

- [ ] **Step 2: Update core __init__.py**

```python
# src/core/__init__.py
"""Core module - shared services and utilities."""
from src.core.config import Settings, get_settings, reset_settings
from src.core.database import execute_query, execute_query_with_columns, get_snowflake_connection
from src.core.llm_provider import UnsupportedProviderError, get_llm

__all__ = [
    "Settings",
    "get_settings",
    "reset_settings",
    "get_snowflake_connection",
    "execute_query",
    "execute_query_with_columns",
    "get_llm",
    "UnsupportedProviderError",
]
```

- [ ] **Step 3: Run all core tests**

Run: `pytest tests/test_core/ -v`
Expected: All tests PASS

- [ ] **Step 4: Commit core module integration**

```bash
git add src/core/__init__.py tests/test_core/test_integration.py
git commit -m "feat(core): Complete core module with integration tests

- Export all components from __init__.py
- Add integration tests for module exports
- Add settings singleton test"
```

---

## Chunk 2: Monitor Module

### Task 6: SQLite Models and Database Initialization

**Files:**
- Create: `src/monitor/__init__.py`
- Create: `src/monitor/models.py`
- Create: `tests/test_monitor/__init__.py`
- Create: `tests/test_monitor/test_models.py`
- Create: `scripts/init_db.py`

- [ ] **Step 1: Write failing test for models**

```python
# tests/test_monitor/test_models.py
"""Tests for monitor models."""
import pytest
import tempfile
import os


def test_metric_model_creation():
    """Test that Metric model can be created."""
    from src.monitor.models import Metric, ThresholdType, ThresholdOperator

    metric = Metric(
        name="Test Metric",
        description="A test metric",
        category="variance",
        sql_template="SELECT 1",
        threshold_type=ThresholdType.PERCENTAGE,
        threshold_value=10.0,
        threshold_operator=ThresholdOperator.GT,
    )

    assert metric.name == "Test Metric"
    assert metric.threshold_type == ThresholdType.PERCENTAGE
    assert metric.is_active is True


def test_metric_result_model():
    """Test that MetricResult model can be created."""
    from src.monitor.models import MetricResult

    result = MetricResult(
        metric_id=1,
        actual_value=15.0,
        threshold_value=10.0,
        is_alert=True,
    )

    assert result.metric_id == 1
    assert result.is_alert is True


def test_alert_queue_model():
    """Test that AlertQueue model can be created."""
    from src.monitor.models import AlertQueue, AlertStatus

    alert = AlertQueue(
        metric_id=1,
        result_id=1,
        status=AlertStatus.PENDING,
    )

    assert alert.status == AlertStatus.PENDING


def test_database_initialization():
    """Test that database tables are created correctly."""
    from src.monitor.models import init_database, Metric, get_session

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        init_database(db_path)

        # Verify we can create and query a metric
        session = get_session(db_path)
        metric = Metric(
            name="Test",
            sql_template="SELECT 1",
            threshold_type="percentage",
            threshold_value=10.0,
            threshold_operator="gt",
        )
        session.add(metric)
        session.commit()

        result = session.query(Metric).first()
        assert result.name == "Test"
        session.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_monitor/test_models.py -v`
Expected: FAIL with "ModuleNotFoundError"

- [ ] **Step 3: Implement models**

Create `src/monitor/models.py` with Metric, MetricResult, AlertQueue models using SQLAlchemy. Include ThresholdType, ThresholdOperator, AlertStatus enums.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_monitor/test_models.py -v`
Expected: All tests PASS

- [ ] **Step 5: Create init_db.py script**

- [ ] **Step 6: Commit**

```bash
git add src/monitor/ tests/test_monitor/ scripts/init_db.py
git commit -m "feat(monitor): Add SQLite models and database initialization"
```

---

### Task 7-9: Metrics Loader, Executor, Alert Engine, Scheduler

*(Detailed steps abbreviated for brevity - follow TDD pattern from previous tasks)*

1. **Metrics Loader**: Load metrics from YAML config into database
2. **SQL Executor**: Execute metric SQL templates against Snowflake
3. **Alert Engine**: Check thresholds, create alerts in queue
4. **Scheduler**: APScheduler-based periodic execution

- [ ] **Commit Monitor Module**

```bash
git commit -m "feat(monitor): Complete monitor module

- Add metrics_loader for YAML config
- Add executor for SQL execution
- Add alert_engine for threshold checking
- Add scheduler with APScheduler
- Add comprehensive tests"
```

---

## Chunk 3: Agent Module

### Task 10-14: LangChain Agent Implementation

**Key Components:**
1. **Agent Factory**: Create LangChain agent with tools and middleware
2. **Snowflake Tool**: Execute SQL queries
3. **Chart Tool**: Generate Plotly charts
4. **Skills**: SQL Analyzer, Data Visualizer
5. **Middleware**: Context enricher for data model injection

- [ ] **Commit Agent Module**

```bash
git commit -m "feat(agent): Add LangChain data analysis agent

- Add agent factory with configurable LLM
- Add SnowflakeTool for query execution
- Add ChartTool for visualization
- Add SQLAnalyzerSkill
- Add ContextEnricherMiddleware
- Add comprehensive tests"
```

---

## Chunk 4: Web UI Module

### Task 15-17: Streamlit Interface

**Pages:**
1. **Chat Page**: Natural language query interface
2. **Monitor Page**: View metrics and alerts
3. **History Page**: View past analyses

- [ ] **Commit Web UI**

```bash
git commit -m "feat(web): Add Streamlit web interface

- Add main app.py entry point
- Add chat component for agent interaction
- Add monitor dashboard page
- Add history page
- Configure Streamlit settings"
```

---

## Chunk 5: Integration & Scripts

### Task 18: Startup Scripts

**Files:**
- Create: `scripts/start_all.sh`

```bash
#!/bin/bash
# Start all services

# Create data directory
mkdir -p data

# Initialize database if needed
python scripts/init_db.py

# Load metrics from config
python -c "from src.monitor import reload_metrics_from_config; reload_metrics_from_config()"

# Start Streamlit (includes monitor in background)
streamlit run src/web/app.py --server.port 8501
```

- [ ] **Step 1: Create startup script**
- [ ] **Step 2: Make executable**
- [ ] **Step 3: Test startup**

### Task 19: Integration Tests

- [ ] **Write end-to-end test for full workflow**
- [ ] **Run integration tests**

### Task 20: Documentation & Final Commit

- [ ] **Update README with usage instructions**
- [ ] **Final commit**

```bash
git add .
git commit -m "chore: Final integration and documentation

- Add startup scripts
- Add integration tests
- Update README with full instructions
- Clean up and polish"
```

---

## Summary

| Phase | Tasks | Estimated Time |
|-------|-------|----------------|
| Chunk 1: Core Module | 1-5 | 2-3 hours |
| Chunk 2: Monitor Module | 6-9 | 3-4 hours |
| Chunk 3: Agent Module | 10-14 | 3-4 hours |
| Chunk 4: Web UI | 15-17 | 2-3 hours |
| Chunk 5: Integration | 18-20 | 1-2 hours |

**Total Estimated: 11-16 hours**

---

**Plan complete and saved to `docs/superpowers/plans/2026-03-16-az-data-agent-implementation.md`. Ready to execute?**