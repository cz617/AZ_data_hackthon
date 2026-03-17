"""
Unit tests for list_rules module.

Tests pattern matching filters and output formatting.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open
from list_rules import (
    load_suite,
    get_suite_info,
    list_rules,
    format_output,
    to_markdown,
    format_suite_markdown,
)


@pytest.fixture
def sample_suite():
    """Sample suite data for testing."""
    return {
        "expectation_suite_name": "orders_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "order_id"},
                "meta": {"rule_id": "r_001", "severity": "error"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"},
                "meta": {"rule_id": "r_002", "severity": "error"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "status", "value_set": ["pending", "completed"]},
                "meta": {"rule_id": "r_003", "severity": "warning"},
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 0, "max_value": 10000},
                "meta": {"rule_id": "r_004", "severity": "info"},
            },
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": "email", "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
                "meta": {"rule_id": "r_005", "severity": "warning"},
            },
        ],
    }


class TestLoadSuite:
    """Tests for load_suite function."""

    @patch("list_rules.Path.exists", return_value=True)
    @patch(
        "builtins.open",
        mock_open(read_data='{"expectation_suite_name": "test_suite", "expectations": []}'),
    )
    def test_load_existing_suite(self, mock_exists):
        """Test loading an existing suite."""
        result = load_suite("test_table")
        assert result["expectation_suite_name"] == "test_suite"

    @patch("list_rules.Path.exists", return_value=False)
    def test_load_nonexistent_suite(self, mock_exists):
        """Test loading a non-existent suite."""
        result = load_suite("nonexistent")
        assert "error" in result


class TestGetSuiteInfo:
    """Tests for get_suite_info function with pattern matching."""

    @patch("list_rules.load_suite")
    def test_no_filters(self, mock_load, sample_suite):
        """Test getting suite info without filters."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders")
        result = json.loads(result_json)

        assert result["table"] == "orders"
        assert result["suite_name"] == "orders_suite"
        assert result["total_rules"] == 5

    @patch("list_rules.load_suite")
    def test_type_pattern_filter(self, mock_load, sample_suite):
        """Test filtering by type pattern with wildcard."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", type_pattern="*unique*")
        result = json.loads(result_json)

        assert result["total_rules"] == 1
        assert result["rules"][0]["expectation_type"] == "expect_column_values_to_be_unique"

    @patch("list_rules.load_suite")
    def test_type_pattern_filter_match_regex(self, mock_load, sample_suite):
        """Test filtering by type pattern matching multiple."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", type_pattern="expect_column_values_to_*")
        result = json.loads(result_json)

        assert result["total_rules"] == 4
        types = {r["expectation_type"] for r in result["rules"]}
        assert "expect_table_row_count_to_be_between" not in types

    @patch("list_rules.load_suite")
    def test_column_pattern_filter(self, mock_load, sample_suite):
        """Test filtering by column pattern."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", column_pattern="*_id")
        result = json.loads(result_json)

        assert result["total_rules"] == 2
        columns = {r["column"] for r in result["rules"]}
        assert columns == {"order_id", "customer_id"}

    @patch("list_rules.load_suite")
    def test_column_pattern_filter_single_char(self, mock_load, sample_suite):
        """Test filtering by column pattern with ? wildcard."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", column_pattern="status")
        result = json.loads(result_json)

        assert result["total_rules"] == 1
        assert result["rules"][0]["column"] == "status"

    @patch("list_rules.load_suite")
    def test_severity_filter(self, mock_load, sample_suite):
        """Test filtering by severity (exact match)."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", severity="error")
        result = json.loads(result_json)

        assert result["total_rules"] == 2
        for rule in result["rules"]:
            assert rule["severity"] == "error"

    @patch("list_rules.load_suite")
    def test_combined_filters(self, mock_load, sample_suite):
        """Test combining multiple filters."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info(
            "orders",
            type_pattern="expect_column_values_to_*",
            column_pattern="*_id",
            severity="error",
        )
        result = json.loads(result_json)

        assert result["total_rules"] == 2

    @patch("list_rules.load_suite")
    def test_filter_no_matches(self, mock_load, sample_suite):
        """Test filter that matches nothing."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", type_pattern="*nonexistent*")
        result = json.loads(result_json)

        assert result["total_rules"] == 0
        assert result["rules"] == []

    @patch("list_rules.load_suite")
    def test_table_row_count_no_column(self, mock_load, sample_suite):
        """Test that table-level expectations work with column filter."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", column_pattern="*")
        result = json.loads(result_json)

        # Table-level expectations have no column, so should be excluded
        # when column filter is applied
        assert result["total_rules"] == 4  # Excludes expect_table_row_count...


class TestOutputFormatting:
    """Tests for output formatting."""

    def test_json_output(self):
        """Test JSON output format."""
        data = {
            "table": "orders",
            "suite_name": "orders_suite",
            "total_rules": 1,
            "rules": [{"rule_id": "r_001", "expectation_type": "expect_column_values_to_be_unique"}],
        }
        result = format_output(data, "json")
        parsed = json.loads(result)

        assert parsed["table"] == "orders"

    def test_markdown_single_suite(self):
        """Test markdown output for single suite."""
        data = {
            "table": "orders",
            "suite_name": "orders_suite",
            "total_rules": 1,
            "rules": [
                {
                    "rule_id": "r_001",
                    "expectation_type": "expect_column_values_to_be_unique",
                    "column": "order_id",
                    "severity": "error",
                }
            ],
        }
        result = to_markdown(data, multi=False)

        assert "## orders" in result
        assert "**Suite**: `orders_suite`" in result
        assert "**Total Rules**: 1" in result
        assert "| Rule ID | Type | Column | Severity |" in result
        assert "| r_001 | `expect_column_values_to_be_unique` | order_id | error |" in result

    def test_markdown_multi_suite(self):
        """Test markdown output for multiple suites."""
        data = [
            {
                "table": "orders",
                "suite_name": "orders_suite",
                "total_rules": 1,
                "rules": [{"rule_id": "r_001", "expectation_type": "exp1", "column": "col1", "severity": "error"}],
            },
            {
                "table": "users",
                "suite_name": "users_suite",
                "total_rules": 1,
                "rules": [{"rule_id": "r_002", "expectation_type": "exp2", "column": "col2", "severity": "warning"}],
            },
        ]
        result = to_markdown(data, multi=True)

        assert "# Data Quality Rules" in result
        assert "## orders" in result
        assert "## users" in result

    def test_markdown_with_null_column(self):
        """Test markdown handles null column values."""
        data = {
            "table": "orders",
            "suite_name": "orders_suite",
            "total_rules": 1,
            "rules": [
                {
                    "rule_id": "r_001",
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "column": None,
                    "severity": "info",
                }
            ],
        }
        result = format_suite_markdown(data)

        assert "| r_001 | `expect_table_row_count_to_be_between` | - | info |" in result

    def test_markdown_with_error(self):
        """Test markdown handles error responses."""
        data = {"error": "Suite not found: nonexistent"}
        result = format_suite_markdown(data)

        assert "## Error: Suite not found: nonexistent" in result


class TestListRules:
    """Tests for list_rules function."""

    @patch("list_rules.load_suite")
    def test_single_table(self, mock_load, sample_suite):
        """Test listing rules for single table."""
        mock_load.return_value = sample_suite
        result_json = list_rules(table="orders")
        result = json.loads(result_json)

        assert result["table"] == "orders"
        assert result["total_rules"] == 5

    @patch("list_rules.Path.exists", return_value=True)
    @patch("list_rules.load_suite")
    def test_all_tables(self, mock_load, mock_exists, sample_suite):
        """Test listing rules for all tables."""
        mock_load.return_value = sample_suite

        with patch.object(Path, "glob", return_value=[Path("artifacts/great_expectations/expectations/orders_suite.json")]):
            result_json = list_rules(all_tables=True)
            result = json.loads(result_json)

            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0]["table"] == "orders"


class TestIntegration:
    """Integration tests for pattern matching."""

    @patch("list_rules.load_suite")
    def test_pattern_match_question_mark(self, mock_load):
        """Test ? wildcard matches single character."""
        suite = {
            "expectation_suite_name": "test_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_a",
                    "kwargs": {"column": "col1"},
                    "meta": {"rule_id": "r_001", "severity": "error"},
                },
                {
                    "expectation_type": "expect_column_b",
                    "kwargs": {"column": "col2"},
                    "meta": {"rule_id": "r_002", "severity": "error"},
                },
                {
                    "expectation_type": "expect_column_cc",
                    "kwargs": {"column": "col3"},
                    "meta": {"rule_id": "r_003", "severity": "error"},
                },
            ],
        }
        mock_load.return_value = suite

        # ? matches exactly one character
        result_json = get_suite_info("test", type_pattern="expect_column_?")
        result = json.loads(result_json)

        assert result["total_rules"] == 2

    @patch("list_rules.load_suite")
    def test_exact_match_still_works(self, mock_load, sample_suite):
        """Test exact match (no wildcards) still works."""
        mock_load.return_value = sample_suite
        result_json = get_suite_info("orders", type_pattern="expect_column_values_to_be_unique")
        result = json.loads(result_json)

        assert result["total_rules"] == 1
