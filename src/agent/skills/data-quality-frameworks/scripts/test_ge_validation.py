"""
Unit tests for ge_validation module.

Tests validation of Great Expectations expectations
and pattern matching utilities.
"""

import pytest
from ge_validation import (
    validate_expectation,
    validate_rules_batch,
    match_pattern,
    GE_BUILTIN_EXPECTATIONS,
    list_available_expectations,
    get_expectation_categories,
)


class TestValidateExpectation:
    """Tests for validate_expectation function."""

    def test_missing_expectation_type(self):
        """Test that missing expectation_type is caught."""
        exp = {"column": "user_id"}
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "Missing required field 'expectation_type'" in error

    def test_unknown_expectation_type(self):
        """Test that unknown expectation types are rejected."""
        exp = {"expectation_type": "unknown_expectation", "column": "user_id"}
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "Unknown expectation type" in error

    def test_missing_required_column(self):
        """Test that missing required column parameter is caught."""
        exp = {"expectation_type": "expect_column_values_to_not_be_null"}
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "Missing required parameter 'column'" in error

    def test_missing_required_value_set(self):
        """Test that missing required value_set parameter is caught."""
        exp = {
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": "status",
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "Missing required parameter 'value_set'" in error

    def test_valid_column_not_null(self):
        """Test valid column not null expectation."""
        exp = {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": "user_id",
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is True
        assert error is None

    def test_valid_column_between(self):
        """Test valid column between expectation with optional params."""
        exp = {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "age",
            "min_value": 0,
            "max_value": 120,
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is True
        assert error is None

    def test_valid_table_row_count_between(self):
        """Test valid table row count expectation (no column required)."""
        exp = {
            "expectation_type": "expect_table_row_count_to_be_between",
            "min_value": 100,
            "max_value": 10000,
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is True
        assert error is None

    def test_invalid_column_type(self):
        """Test that wrong parameter type is caught."""
        exp = {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": 123,  # Should be string
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "must be str" in error

    def test_invalid_value_set_type(self):
        """Test that wrong value_set type is caught."""
        exp = {
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": "status",
            "value_set": "active",  # Should be list
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "must be list" in error

    def test_invalid_min_max_consistency(self):
        """Test that min_value > max_value is caught."""
        exp = {
            "expectation_type": "expect_column_values_to_be_between",
            "column": "age",
            "min_value": 100,
            "max_value": 10,
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "min_value" in error and "max_value" in error

    def test_invalid_mostly_range(self):
        """Test that mostly outside [0, 1] is caught."""
        exp = {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": "user_id",
            "mostly": 1.5,
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "mostly" in error and "between 0 and 1" in error

    def test_empty_value_set(self):
        """Test that empty value_set is caught."""
        exp = {
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": "status",
            "value_set": [],
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "'value_set' cannot be empty" in error

    def test_unknown_parameter(self):
        """Test that unknown parameters are caught."""
        exp = {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": "user_id",
            "unknown_param": "value",
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is False
        assert "Unknown parameters" in error

    def test_column_pair_expectation(self):
        """Test column pair comparison expectations."""
        exp = {
            "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
            "column_A": "price",
            "column_B": "cost",
            "or_equal": True,
        }
        is_valid, error = validate_expectation(exp)
        assert is_valid is True
        assert error is None


class TestValidateRulesBatch:
    """Tests for validate_rules_batch function."""

    def test_all_valid_rules(self):
        """Test batch validation with all valid rules."""
        rules = [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "user_id",
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "column": "age",
                "min_value": 0,
                "max_value": 120,
            },
        ]
        valid, invalid = validate_rules_batch(rules)
        assert len(valid) == 2
        assert len(invalid) == 0

    def test_all_invalid_rules(self):
        """Test batch validation with all invalid rules."""
        rules = [
            {"expectation_type": "expect_column_values_to_not_be_null"},
            {"expectation_type": "unknown_expectation", "column": "user_id"},
        ]
        valid, invalid = validate_rules_batch(rules)
        assert len(valid) == 0
        assert len(invalid) == 2
        assert all("error" in inv for inv in invalid)

    def test_mixed_valid_invalid_rules(self):
        """Test batch validation with mixed valid/invalid rules."""
        rules = [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "column": "user_id",
            },
            {"expectation_type": "expect_column_values_to_not_be_null"},
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "column": "status",
                "value_set": ["active", "inactive"],
            },
        ]
        valid, invalid = validate_rules_batch(rules)
        assert len(valid) == 2
        assert len(invalid) == 1
        assert "Missing required parameter 'column'" in invalid[0]["error"]

    def test_empty_rules_list(self):
        """Test batch validation with empty list."""
        valid, invalid = validate_rules_batch([])
        assert len(valid) == 0
        assert len(invalid) == 0


class TestMatchPattern:
    """Tests for match_pattern function."""

    def test_wildcard_asterisk(self):
        """Test * wildcard matching."""
        assert match_pattern("table_users", "table_*") is True
        assert match_pattern("table_", "table_*") is True  # * matches zero chars after _
        assert match_pattern("table", "table*") is True  # * matches zero chars
        assert match_pattern("user", "table_*") is False

    def test_wildcard_question_mark(self):
        """Test ? wildcard matching."""
        assert match_pattern("column_id", "column_??") is True
        assert match_pattern("column_i", "column_??") is False

    def test_exact_match(self):
        """Test exact string matching."""
        assert match_pattern("status", "status") is True
        assert match_pattern("state", "status") is False

    def test_combined_wildcards(self):
        """Test combined wildcards."""
        assert match_pattern("user_email", "user_*") is True
        assert match_pattern("user_123_email", "user_*") is True

    def test_special_regex_chars(self):
        """Test that special regex characters are escaped."""
        assert match_pattern("column.id", "column.id") is True
        assert match_pattern("column$id", "column$id") is True


class TestGEBuiltInExpectations:
    """Tests for GE_BUILTIN_EXPECTATIONS dictionary."""

    def test_expectations_count(self):
        """Test that we have the expected number of expectations."""
        assert len(GE_BUILTIN_EXPECTATIONS) == 24

    def test_all_expectations_have_required_field(self):
        """Test that all expectations have 'required' field."""
        for exp_type, exp_def in GE_BUILTIN_EXPECTATIONS.items():
            assert "required" in exp_def, f"{exp_type} missing 'required' field"

    def test_all_expectations_have_optional_field(self):
        """Test that all expectations have 'optional' field."""
        for exp_type, exp_def in GE_BUILTIN_EXPECTATIONS.items():
            assert "optional" in exp_def, f"{exp_type} missing 'optional' field"

    def test_all_expectations_have_param_types(self):
        """Test that all expectations have 'param_types' field."""
        for exp_type, exp_def in GE_BUILTIN_EXPECTATIONS.items():
            assert "param_types" in exp_def, f"{exp_type} missing 'param_types' field"


class TestListAvailableExpectations:
    """Tests for list_available_expectations function."""

    def test_returns_list(self):
        """Test that function returns a list."""
        result = list_available_expectations()
        assert isinstance(result, list)

    def test_returns_sorted_list(self):
        """Test that function returns sorted list."""
        result = list_available_expectations()
        assert result == sorted(result)

    def test_contains_all_expectations(self):
        """Test that all expectations are listed."""
        result = list_available_expectations()
        assert len(result) == 24


class TestGetExpectationCategories:
    """Tests for get_expectation_categories function."""

    def test_returns_dict(self):
        """Test that function returns a dict."""
        result = get_expectation_categories()
        assert isinstance(result, dict)

    def test_all_categories_exist(self):
        """Test that all expected categories exist."""
        categories = get_expectation_categories()
        expected_categories = [
            "schema",
            "row_count",
            "null",
            "unique",
            "range",
            "string",
            "date",
            "statistical",
            "set_operations",
            "comparison",
            "aggregate",
        ]
        for cat in expected_categories:
            assert cat in categories

    def test_category_values_are_lists(self):
        """Test that category values are lists."""
        categories = get_expectation_categories()
        for cat, exps in categories.items():
            assert isinstance(exps, list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
