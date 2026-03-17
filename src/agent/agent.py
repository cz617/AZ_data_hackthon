"""Main agent factory for creating the data analysis agent using DeepAgent."""
from typing import Optional

from deepagents import create_deep_agent

from src.core.config import Settings, get_settings
from src.agent.tools import get_default_tools
from src.agent.middleware import DataContextMiddleware
from src.agent.skills import get_skill_paths


SYSTEM_PROMPT = """You are an AI data analyst for AstraZeneca pharmaceutical data.

Your role is to help users analyze business data, answer questions about financial metrics,
market performance, and generate insights from the data warehouse.

## Core Capabilities

1. **SQL Analysis**: Generate and execute Snowflake queries
2. **Data Visualization**: Create charts and graphs from query results
3. **Report Generation**: Produce structured analysis reports

## Workflow

1. Understand the user's question completely
2. Generate appropriate SQL queries using the snowflake_query tool
3. Analyze the results and explain findings
4. Offer to create visualizations when helpful
5. Generate reports for complex analysis

## Guidelines

- Always use full table paths: ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>
- Use NULLIF to prevent division by zero
- Limit query results to reasonable sizes
- Explain your analysis in business terms
"""


def create_az_data_agent(
    settings: Optional[Settings] = None,
    verbose: bool = False,
):
    """
    Create the AZ data analysis agent using DeepAgent.

    Args:
        settings: Application settings (uses global if not provided)
        verbose: Whether to print agent reasoning (not used in DeepAgent)

    Returns:
        CompiledStateGraph (DeepAgent instance)
    """
    settings = settings or get_settings()

    # Initialize tools (function-based)
    tools = get_default_tools()

    # Initialize middleware
    middleware = [
        DataContextMiddleware(),
    ]

    # Get skill directory paths
    skills = get_skill_paths()

    # Create DeepAgent
    agent = create_deep_agent(
        model=settings.llm_model,
        tools=tools,
        middleware=middleware,
        skills=skills,
        system_prompt=SYSTEM_PROMPT,
    )

    return agent


def analyze_with_agent(
    question: str,
    settings: Optional[Settings] = None,
) -> str:
    """
    Analyze a question using the data agent.

    Args:
        question: User's question about the data
        settings: Application settings

    Returns:
        Agent's response
    """
    agent = create_az_data_agent(settings)
    result = agent.invoke({
        "messages": [{"role": "user", "content": question}]
    })
    # DeepAgent returns result in result["messages"][-1].content
    if result and "messages" in result and result["messages"]:
        return result["messages"][-1].content
    return "Unable to generate response"


# Backward compatibility alias
create_data_agent = create_az_data_agent


__all__ = [
    "create_az_data_agent",
    "create_data_agent",
    "analyze_with_agent",
]