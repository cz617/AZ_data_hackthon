#!/usr/bin/env python3
"""
Configure Great Expectations environment.

Provides commands to initialize, check status, and reset GE structure.

Usage:
    # Initialize GE structure
    python config_ge.py --init

    # Check GE status
    python config_ge.py --status

    # Reset (delete and reinitialize)
    python config_ge.py --reset

Creates artifacts/great_expectations/ with standard GE layout:
- expectations/       # Suite JSON files
- checkpoints/        # Checkpoint configurations
- great_expectations.yml  # GE config
"""

import argparse
import json
import shutil
import sys
from pathlib import Path


def get_ge_dir(project_dir: Path) -> Path:
    """Get the GE directory path."""
    return project_dir / "artifacts" / "great_expectations"


def get_status(project_dir: Path) -> dict:
    """Get GE environment status as JSON."""
    ge_dir = get_ge_dir(project_dir)
    config_file = ge_dir / "great_expectations.yml"
    expectations_dir = ge_dir / "expectations"

    # Check if initialized
    is_initialized = config_file.exists()

    # Count suites
    suites = []
    if expectations_dir.exists():
        for suite_file in expectations_dir.glob("*_suite.json"):
            # Extract suite name without _suite suffix
            suite_name = suite_file.stem.replace("_suite", "")
            suites.append(suite_name)

    return {
        "status": "initialized" if is_initialized else "not_initialized",
        "ge_dir": str(ge_dir),
        "config_exists": config_file.exists(),
        "suites_count": len(suites),
        "suites": sorted(suites),
    }


def create_ge_structure(project_dir: Path) -> dict:
    """Create GE standard directory structure."""
    ge_dir = get_ge_dir(project_dir)

    # Create directories
    (ge_dir / "expectations").mkdir(parents=True, exist_ok=True)
    (ge_dir / "checkpoints").mkdir(parents=True, exist_ok=True)

    # Create great_expectations.yml
    config_yml = ge_dir / "great_expectations.yml"
    if not config_yml.exists():
        config_content = """# Great Expectations configuration
# This is a minimal config for stateless validation

datasource:
  name: project_datasource
  class_name: Datasource
  execution_engine:
    class_name: PandasExecutionEngine
  data_connectors:
    default_inferred_data_connector:
      class_name: InferredAssetFilesystemDataConnector
      base_directory: ..
      default_regex:
        group_names:
          - data_asset_name
        pattern: (.*)\\.csv

# Store backends (filesystem-based)
expectations_store:
  class_name: ExpectationsStore
  store_backend:
    class_name: TupleFilesystemStoreBackend
    base_directory: expectations/

checkpoint_store:
  class_name: CheckpointStore
  store_backend:
    class_name: TupleFilesystemStoreBackend
    base_directory: checkpoints/

# No validations_store - we generate reports on demand
"""
        config_yml.write_text(config_content, encoding="utf-8")

    return get_status(project_dir)


def reset_ge_structure(project_dir: Path) -> dict:
    """Delete and reinitialize GE structure."""
    ge_dir = get_ge_dir(project_dir)

    # Delete existing structure
    if ge_dir.exists():
        shutil.rmtree(ge_dir)

    # Reinitialize
    return create_ge_structure(project_dir)


def main():
    parser = argparse.ArgumentParser(
        description="Configure Great Expectations environment"
    )
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=Path("."),
        help="Project directory (default: current directory)",
    )

    # Mutually exclusive commands
    command_group = parser.add_mutually_exclusive_group(required=True)
    command_group.add_argument(
        "--init",
        action="store_true",
        help="Initialize GE structure",
    )
    command_group.add_argument(
        "--status",
        action="store_true",
        help="Check GE status (JSON output)",
    )
    command_group.add_argument(
        "--reset",
        action="store_true",
        help="Delete and reinitialize GE structure",
    )

    args = parser.parse_args()

    # Validate project directory
    if not (args.project_dir / ".amandax").exists():
        result = {
            "error": f"{args.project_dir} is not a valid AmandaX project",
            "status": "error",
        }
        print(json.dumps(result, indent=2))
        sys.exit(1)

    # Execute command
    if args.status:
        result = get_status(args.project_dir)
    elif args.init:
        result = create_ge_structure(args.project_dir)
    elif args.reset:
        result = reset_ge_structure(args.project_dir)
    else:
        # This should not happen due to mutually_exclusive_group(required=True)
        result = {"error": "No command specified", "status": "error"}

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
