#!/usr/bin/env python3
"""
Tests for upsert_rules.py
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open
from datetime import datetime

# Import the module to test
from upsert_rules import (
    generate_rule_id,
    load_suite,
    save_suite,
    upsert_rules,
    main,
)


class TestGenerateRuleId:
    """Test rule ID generation."""

    def test_generates_consistent_id(self):
        """Same inputs should produce same ID."""
        id1 = generate_rule_id("expect_column_values_to_be_unique", "order_id")
        id2 = generate_rule_id("expect_column_values_to_be_unique", "order_id")
        assert id1 == id2

    def test_different_inputs_produce_different_ids(self):
        """Different inputs should produce different IDs."""
        id1 = generate_rule_id("expect_column_values_to_be_unique", "order_id")
        id2 = generate_rule_id("expect_column_values_to_be_unique", "customer_id")
        assert id1 != id2

    def test_format_is_correct(self):
        """ID should start with 'r_' and be short."""
        rule_id = generate_rule_id("expect_column_values_to_be_unique", "order_id")
        assert rule_id.startswith("r_")
        assert len(rule_id) == 8  # 'r_' + 6 hex chars


class TestLoadSuite:
    """Test suite loading."""

    def test_loads_existing_suite(self, tmp_path):
        """Should load existing suite file."""
        suite_data = {
            "data_asset_type": None,
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"rule_id": "r_abc123"},
                }
            ],
            "meta": {"great_expectations_version": "0.18.0"},
        }

        suite_path = tmp_path / "expectations" / "orders_suite.json"
        suite_path.parent.mkdir(parents=True)
        suite_path.write_text(json.dumps(suite_data))

        with patch("upsert_rules.GE_DIR", tmp_path):
            result = load_suite("orders")
            assert result["expectation_suite_name"] == "orders_suite"
            assert len(result["expectations"]) == 1

    def test_creates_new_suite(self, tmp_path):
        """Should create new suite if not exists."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            result = load_suite("new_table")
            assert result["expectation_suite_name"] == "new_table_suite"
            assert result["expectations"] == []
            assert "great_expectations_version" in result["meta"]


class TestSaveSuite:
    """Test suite saving."""

    def test_saves_to_correct_path(self, tmp_path):
        """Should save suite to correct path."""
        suite = {
            "expectation_suite_name": "orders_suite",
            "expectations": [],
            "meta": {},
        }

        with patch("upsert_rules.GE_DIR", tmp_path):
            path = save_suite("orders", suite)
            assert path.exists()
            assert path.name == "orders_suite.json"

            # Verify content
            with open(path) as f:
                saved = json.load(f)
            assert saved["expectation_suite_name"] == "orders_suite"


class TestUpsertRules:
    """Test upsert rules functionality."""

    def test_validates_rules_before_upsert(self, tmp_path):
        """Should validate all rules before processing."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            # Mix of valid and invalid rules
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    # Missing 'column' - invalid
                },
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is False
            assert "validation_errors" in result
            assert len(result["validation_errors"]) == 1
            assert result["valid_rules"] == 1
            assert result["invalid_rules"] == 1

    def test_adds_new_rules(self, tmp_path):
        """Should add new rules to suite."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "customer_id"},
                },
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is True
            assert result["added"] == 2
            assert result["updated"] == 0
            assert result["total_expectations"] == 2
            assert len(result["added_rules"]) == 2

    def test_updates_existing_rules(self, tmp_path):
        """Should update existing rules with same (type, column)."""
        # Create existing suite
        existing_suite = {
            "data_asset_type": None,
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"rule_id": "r_old123", "severity": "error"},
                }
            ],
            "meta": {},
        }

        expectations_dir = tmp_path / "expectations"
        expectations_dir.mkdir(parents=True)
        suite_path = expectations_dir / "orders_suite.json"
        suite_path.write_text(json.dumps(existing_suite))

        with patch("upsert_rules.GE_DIR", tmp_path):
            # Update with same (type, column) but different severity
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"severity": "warning"},
                }
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is True
            assert result["added"] == 0
            assert result["updated"] == 1
            assert len(result["updated_rules"]) == 1

    def test_mixed_add_and_update(self, tmp_path):
        """Should handle both add and update in one call."""
        existing_suite = {
            "data_asset_type": None,
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"rule_id": "r_old123"},
                }
            ],
            "meta": {},
        }

        expectations_dir = tmp_path / "expectations"
        expectations_dir.mkdir(parents=True)
        suite_path = expectations_dir / "orders_suite.json"
        suite_path.write_text(json.dumps(existing_suite))

        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                # Update existing
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"severity": "warning"},
                },
                # Add new
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "customer_id"},
                },
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is True
            assert result["added"] == 1
            assert result["updated"] == 1
            assert result["total_expectations"] == 2

    def test_dry_run_does_not_save(self, tmp_path):
        """Dry run should validate but not save."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                }
            ]

            result = upsert_rules("orders", rules, dry_run=True)

            assert result["success"] is True
            assert result["dry_run"] is True
            # Suite file should not exist
            suite_path = tmp_path / "expectations" / "orders_suite.json"
            assert not suite_path.exists()

    def test_validation_error_format(self, tmp_path):
        """Validation errors should have correct format."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    # Missing column
                }
            ]

            result = upsert_rules("orders", rules)

            assert "validation_errors" in result
            error = result["validation_errors"][0]
            assert "index" in error
            assert "rule" in error
            assert "error" in error
            assert error["index"] == 0

    def test_single_object_wrapped_in_array(self, tmp_path):
        """Single rule object should be wrapped in array."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            # Pass single dict instead of list
            single_rule = {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "order_id"},
            }

            result = upsert_rules("orders", single_rule)

            assert result["success"] is True
            assert result["added"] == 1

    def test_generates_rule_id_if_missing(self, tmp_path):
        """Should generate rule_id if not provided."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                }
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is True
            added_rule = result["added_rules"][0]
            assert added_rule["rule_id"].startswith("r_")

    def test_preserves_custom_rule_id(self, tmp_path):
        """Should preserve custom rule_id if provided."""
        expectations_dir = tmp_path / "expectations"
        expectations_dir.mkdir(parents=True)

        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"rule_id": "custom_rule_001"},
                }
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is True
            added_rule = result["added_rules"][0]
            assert added_rule["rule_id"] == "custom_rule_001"


class TestMain:
    """Test CLI main function."""

    def test_reads_from_rules_arg(self, tmp_path):
        """Should read rules from --rules argument."""
        # Create expectations directory
        expectations_dir = tmp_path / "expectations"
        expectations_dir.mkdir(parents=True)

        # Create .amandax directory for project check
        amandax_dir = tmp_path / ".amandax"
        amandax_dir.mkdir()

        with patch("upsert_rules.GE_DIR", tmp_path):
            with patch("sys.argv", [
                "upsert_rules.py",
                "--table", "orders",
                "--rules", '[{"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}}]',
            ]):
                # Mock Path.exists to return True for .amandax check
                original_exists = Path.exists

                def mock_exists(self):
                    if ".amandax" in str(self):
                        return True
                    return original_exists(self)

                with patch.object(Path, "exists", mock_exists):
                    # Capture stdout
                    import io
                    from contextlib import redirect_stdout

                    output = io.StringIO()
                    with redirect_stdout(output):
                        try:
                            main()
                        except SystemExit as e:
                            if e.code != 0:
                                raise

                    result = json.loads(output.getvalue())
                    assert result["success"] is True
                    assert result["added"] == 1

    def test_reads_from_file(self, tmp_path):
        """Should read rules from --file argument."""
        rules_file = tmp_path / "rules.json"
        rules_file.write_text(json.dumps([
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "order_id"},
            }
        ]))

        # Create expectations directory
        expectations_dir = tmp_path / "expectations"
        expectations_dir.mkdir(parents=True)

        # Create .amandax directory for project check
        amandax_dir = tmp_path / ".amandax"
        amandax_dir.mkdir()

        with patch("upsert_rules.GE_DIR", tmp_path):
            with patch("sys.argv", [
                "upsert_rules.py",
                "--table", "orders",
                "--file", str(rules_file),
            ]):
                original_exists = Path.exists

                def mock_exists(self):
                    if ".amandax" in str(self):
                        return True
                    return original_exists(self)

                with patch.object(Path, "exists", mock_exists):
                    import io
                    from contextlib import redirect_stdout

                    output = io.StringIO()
                    with redirect_stdout(output):
                        try:
                            main()
                        except SystemExit as e:
                            if e.code != 0:
                                raise

                    result = json.loads(output.getvalue())
                    assert result["success"] is True
                    assert result["added"] == 1

    def test_dry_run_flag(self, tmp_path):
        """Should support --dry-run flag."""
        # Create expectations directory
        expectations_dir = tmp_path / "expectations"
        expectations_dir.mkdir(parents=True)

        # Create .amandax directory for project check
        amandax_dir = tmp_path / ".amandax"
        amandax_dir.mkdir()

        with patch("upsert_rules.GE_DIR", tmp_path):
            with patch("sys.argv", [
                "upsert_rules.py",
                "--table", "orders",
                "--rules", '[{"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}}]',
                "--dry-run",
            ]):
                original_exists = Path.exists

                def mock_exists(self):
                    if ".amandax" in str(self):
                        return True
                    return original_exists(self)

                with patch.object(Path, "exists", mock_exists):
                    import io
                    from contextlib import redirect_stdout

                    output = io.StringIO()
                    with redirect_stdout(output):
                        try:
                            main()
                        except SystemExit as e:
                            if e.code != 0:
                                raise

                    result = json.loads(output.getvalue())
                    assert result["dry_run"] is True


class TestValidationErrors:
    """Test validation error scenarios."""

    def test_missing_expectation_type(self, tmp_path):
        """Should reject rule without expectation_type."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "kwargs": {"column": "order_id"},
                }
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is False
            assert "Missing required field 'expectation_type'" in result["validation_errors"][0]["error"]

    def test_unknown_expectation_type(self, tmp_path):
        """Should reject unknown expectation type."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_something_crazy",
                    "kwargs": {"column": "order_id"},
                }
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is False
            assert "Unknown expectation type" in result["validation_errors"][0]["error"]

    def test_missing_required_column(self, tmp_path):
        """Should reject rule missing required column parameter."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    # Missing column in kwargs
                }
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is False
            assert "Missing required parameter 'column'" in result["validation_errors"][0]["error"]

    def test_multiple_validation_errors(self, tmp_path):
        """Should report all validation errors."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    # Missing column
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "status"},
                    # Missing value_set
                },
            ]

            result = upsert_rules("orders", rules)

            assert result["success"] is False
            assert result["valid_rules"] == 0
            assert result["invalid_rules"] == 2
            assert len(result["validation_errors"]) == 2

    def test_hint_in_error_response(self, tmp_path):
        """Should include hint for validation errors."""
        with patch("upsert_rules.GE_DIR", tmp_path):
            rules = [
                {
                    "expectation_type": "unknown_type",
                    "kwargs": {"column": "order_id"},
                }
            ]

            result = upsert_rules("orders", rules)

            assert "hint" in result
            assert "GE built-in expectations" in result["hint"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
