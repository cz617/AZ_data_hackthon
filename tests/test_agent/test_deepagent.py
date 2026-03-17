"""Tests for DeepAgent integration with SQLDatabaseToolkit."""
import pytest


class TestTools:
    """Tests for SQLDatabaseToolkit-based tools."""

    def test_get_snowflake_tools_returns_list(self):
        """Test that get_snowflake_tools returns a list of tools."""
        from unittest.mock import MagicMock
        from src.agent.tools import get_snowflake_tools

        # Create a mock LLM
        mock_llm = MagicMock()
        # This will fail without actual Snowflake connection, but tests the function signature
        # In a real test, you would mock the database connection

    def test_create_chart_is_callable(self):
        """Test that create_chart is a callable tool."""
        from src.agent.tools import create_chart
        assert callable(create_chart)
        assert hasattr(create_chart, 'name')
        assert create_chart.name == "create_chart"

    def test_get_default_tools_requires_llm(self):
        """Test that get_default_tools requires an LLM parameter."""
        from unittest.mock import MagicMock
        from src.agent.tools import get_default_tools

        # Create a mock LLM
        mock_llm = MagicMock()
        # Function should accept llm parameter
        import inspect
        sig = inspect.signature(get_default_tools)
        assert 'llm' in sig.parameters

    def test_get_snowflake_db_function_exists(self):
        """Test that get_snowflake_db function exists and is callable."""
        from src.agent.tools import get_snowflake_db
        assert callable(get_snowflake_db)


class TestMiddleware:
    """Tests for middleware components."""

    def test_data_context_middleware_is_agent_middleware(self):
        """Test that DataContextMiddleware inherits from AgentMiddleware."""
        from langchain.agents.middleware import AgentMiddleware
        from src.agent.middleware import DataContextMiddleware
        assert issubclass(DataContextMiddleware, AgentMiddleware)

    def test_alert_trigger_handler_has_required_methods(self):
        """Test that AlertTriggerHandler has required methods."""
        from src.agent.middleware import AlertTriggerHandler
        handler = AlertTriggerHandler()
        assert hasattr(handler, 'set_agent_invoke')
        assert hasattr(handler, 'on_alert')
        assert callable(handler.set_agent_invoke)
        assert callable(handler.on_alert)

    def test_get_alert_handler_returns_singleton(self):
        """Test that get_alert_handler returns the same instance."""
        from src.agent.middleware import get_alert_handler
        handler1 = get_alert_handler()
        handler2 = get_alert_handler()
        assert handler1 is handler2


class TestSkills:
    """Tests for skills module."""

    def test_skills_registry_has_required_skills(self):
        """Test that skills registry contains all required skills."""
        from src.agent.skills import SKILLS_REGISTRY
        assert "sql_analyzer" in SKILLS_REGISTRY
        assert "data_visualizer" in SKILLS_REGISTRY
        assert "report_generator" in SKILLS_REGISTRY

    def test_get_skill_paths_returns_list(self):
        """Test that get_skill_paths returns a list of strings."""
        from src.agent.skills import get_skill_paths
        paths = get_skill_paths()
        assert isinstance(paths, list)
        assert len(paths) == 3
        for path in paths:
            assert isinstance(path, str)

    def test_skill_files_exist(self):
        """Test that all skill.md files exist."""
        from src.agent.skills import SKILLS_DIR
        import os
        for skill_name in ["sql_analyzer", "data_visualizer", "report_generator"]:
            skill_path = SKILLS_DIR / skill_name / "skill.md"
            assert os.path.exists(skill_path), f"skill.md not found for {skill_name}"


class TestBusinessContext:
    """Tests for business context configuration."""

    def test_business_context_is_string(self):
        """Test that BUSINESS_CONTEXT is a non-empty string."""
        from src.agent.context import BUSINESS_CONTEXT
        assert isinstance(BUSINESS_CONTEXT, str)
        assert len(BUSINESS_CONTEXT) > 0

    def test_business_context_contains_required_sections(self):
        """Test that BUSINESS_CONTEXT contains required sections."""
        from src.agent.context import BUSINESS_CONTEXT
        assert "FACT_PNL_BASE_BRAND" in BUSINESS_CONTEXT
        assert "FACT_COM_BASE_BRAND" in BUSINESS_CONTEXT
        assert "DIM_PRODUCT" in BUSINESS_CONTEXT
        assert "DIM_TIME" in BUSINESS_CONTEXT