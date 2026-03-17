"""Data Agent module for AZ Data Agent."""
from src.agent.agent import (
    create_az_data_agent,
    create_data_agent,
    analyze_with_agent,
)
from src.agent.middleware import (
    DataContextMiddleware,
    ContextEnricherMiddleware,
    AlertTriggerHandler,
    get_alert_handler,
)
from src.agent.tools import (
    get_snowflake_tools,
    get_snowflake_db,
    create_chart,
    get_default_tools,
)
from src.agent.skills import (
    SKILLS_REGISTRY,
    get_skill_paths,
    SKILLS_DIR,
)

__all__ = [
    # Agent factory
    "create_az_data_agent",
    "create_data_agent",
    "analyze_with_agent",
    # Middleware
    "DataContextMiddleware",
    "ContextEnricherMiddleware",
    "AlertTriggerHandler",
    "get_alert_handler",
    # Tools
    "get_snowflake_tools",
    "get_snowflake_db",
    "create_chart",
    "get_default_tools",
    # Skills
    "SKILLS_REGISTRY",
    "get_skill_paths",
    "SKILLS_DIR",
]