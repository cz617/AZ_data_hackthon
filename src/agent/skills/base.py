"""Base class for agent skills."""
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseSkill(ABC):
    """Base class for agent skills."""

    name: str = ""
    description: str = ""
    tools: list = []

    @abstractmethod
    def get_prompt(self) -> str:
        """Return the skill prompt for the agent."""
        pass

    def get_tools(self) -> list:
        """Return tools associated with this skill."""
        return self.tools