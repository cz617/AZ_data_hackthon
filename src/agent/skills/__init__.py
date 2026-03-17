"""Skills module for the data agent."""
from pathlib import Path

SKILLS_DIR = Path(__file__).parent

SKILLS_REGISTRY = {
    "sql_analyzer": str(SKILLS_DIR / "sql_analyzer"),
    "data_visualizer": str(SKILLS_DIR / "data_visualizer"),
    "report_generator": str(SKILLS_DIR / "report_generator"),
}


def get_skill_paths() -> list[str]:
    """Get all skill directory paths for DeepAgent to load."""
    return list(SKILLS_REGISTRY.values())


__all__ = ["SKILLS_REGISTRY", "get_skill_paths", "SKILLS_DIR"]