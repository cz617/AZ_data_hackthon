"""Main agent factory for creating the data analysis agent."""
from typing import Optional

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate

from src.core.config import Settings, get_settings
from src.core.llm_provider import get_llm
from src.agent.tools import SnowflakeTool, ChartTool
from src.agent.skills import SQLAnalyzerSkill
from src.agent.middleware import ContextEnricherMiddleware


SYSTEM_PROMPT = """You are an AI data analyst for AstraZeneca pharmaceutical data.

Your role is to help users analyze business data, answer questions about financial metrics,
market performance, and generate insights from the data warehouse.

You have access to:
- Snowflake data warehouse with P&L and commercial data
- Chart creation capabilities

Always:
1. Understand the user's question fully before querying
2. Use appropriate SQL queries with proper table joins
3. Explain your findings clearly
4. Offer to create visualizations when helpful

{context}
"""


def create_data_agent(
    settings: Optional[Settings] = None,
    verbose: bool = False,
) -> AgentExecutor:
    """
    Create a data analysis agent.

    Args:
        settings: Application settings (uses global if not provided)
        verbose: Whether to print agent reasoning

    Returns:
        Configured AgentExecutor
    """
    settings = settings or get_settings()

    # Initialize LLM
    llm = get_llm(settings)

    # Initialize tools
    tools = [
        SnowflakeTool(),
        ChartTool(),
    ]

    # Get context from middleware
    middleware = ContextEnricherMiddleware()
    context = middleware.DATA_CONTEXT

    # Create prompt
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT.format(context=context)),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ])

    # Create agent
    agent = create_tool_calling_agent(llm, tools, prompt)

    # Create executor
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=verbose,
        handle_parsing_errors=True,
    )

    return agent_executor


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
    agent = create_data_agent(settings)
    result = agent.invoke({"input": question})
    return result.get("output", "Unable to generate response")