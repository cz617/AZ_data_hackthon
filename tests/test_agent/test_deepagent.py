"""Tests for DeepAgent integration."""
import pytest


class TestTools:
    """Tests for function-based tools."""

    def test_snowflake_query_is_callable(self):
        """Test that snowflake_query is a callable tool."""
        from src.agent.tools import snowflake_query
        assert callable(snowflake_query)
        assert hasattr(snowflake_query, 'name')
        assert snowflake_query.name == "snowflake_query"

    def test_create_chart_is_callable(self):
        """Test that create_chart is a callable tool."""
        from src.agent.tools import create_chart
        assert callable(create_chart)
        assert hasattr(create_chart, 'name')
        assert create_chart.name == "create_chart"

    def test_list_tables_is_callable(self):
        """Test that list_tables is a callable tool."""
        from src.agent.tools import list_tables
        assert callable(list_tables)
        assert hasattr(list_tables, 'name')
        assert list_tables.name == "list_tables"

    def test_describe_table_is_callable(self):
        """Test that describe_table is a callable tool."""
        from src.agent.tools import describe_table
        assert callable(describe_table)
        assert hasattr(describe_table, 'name')
        assert describe_table.name == "describe_table"

    def test_preview_table_is_callable(self):
        """Test that preview_table is a callable tool."""
        from src.agent.tools import preview_table
        assert callable(preview_table)
        assert hasattr(preview_table, 'name')
        assert preview_table.name == "preview_table"

    def test_get_table_stats_is_callable(self):
        """Test that get_table_stats is a callable tool."""
        from src.agent.tools import get_table_stats
        assert callable(get_table_stats)
        assert hasattr(get_table_stats, 'name')
        assert get_table_stats.name == "get_table_stats"

    def test_get_default_tools_returns_list(self):
        """Test that get_default_tools returns a list of tools."""
        from src.agent.tools import get_default_tools
        tools = get_default_tools()
        assert isinstance(tools, list)
        assert len(tools) == 6  # 4 exploration + 1 query + 1 chart


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