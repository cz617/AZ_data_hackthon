"""Data context middleware for injecting business context into agent."""
from typing import Callable

from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage

from src.agent.context.business_context import BUSINESS_CONTEXT


class DataContextMiddleware(AgentMiddleware):
    """
    Middleware that injects business context into the agent's system prompt.

    This middleware appends the business context (table structures, field descriptions,
    business metadata) to the system message content blocks before each LLM call.
    """

    def __init__(self):
        super().__init__()
        self.context = BUSINESS_CONTEXT

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        """
        Inject context into the system message.

        Appends business context to the system_message content_blocks.
        """
        # Get current system message content blocks
        current_blocks = list(request.system_message.content_blocks)

        # Add business context block
        context_block = {
            "type": "text",
            "text": f"\n\n## 数据仓库上下文\n\n{self.context}"
        }
        new_blocks = current_blocks + [context_block]

        # Create new system message
        new_system_message = SystemMessage(content=new_blocks)

        # Call handler with modified request
        return handler(request.override(system_message=new_system_message))


# Keep backward compatibility alias
ContextEnricherMiddleware = DataContextMiddleware


__all__ = ["DataContextMiddleware", "ContextEnricherMiddleware"]