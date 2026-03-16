#!/usr/bin/env python
"""Initialize the monitor database."""
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from src.monitor.models import init_database


def main():
    """Initialize the database with default tables."""
    db_path = "data/monitor.db"
    print(f"Initializing database at {db_path}...")
    init_database(db_path)
    print("Database initialized successfully!")


if __name__ == "__main__":
    main()