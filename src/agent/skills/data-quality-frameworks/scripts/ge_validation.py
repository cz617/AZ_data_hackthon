"""
Shared validation module for Great Expectations expectations.

Provides validation utilities for GE built-in expectation types
and pattern matching utilities for data quality rules.
"""

import re
from typing import Any, Dict, List, Optional, Tuple


# GE Built-in Expectations Dictionary
# Format: {
#     "expectation_type": {
#         "required": ["param1", "param2"],
#         "optional": ["param3"],
#         "param_types": {"param1": str, "param2": (int, float), ...}
#     }
# }
GE_BUILTIN_EXPECTATIONS: Dict[str, Dict[str, Any]] = {
    # Schema Expectations
    "expect_table_columns_to_match_set": {
        "required": ["column_set"],
        "optional": ["exact_match"],
        "param_types": {
            "column_set": list,
            "exact_match": bool,
        },
    },
    "expect_table_column_count_to_equal": {
        "required": ["value"],
        "optional": [],
        "param_types": {
            "value": int,
        },
    },
    # Row Count Expectations
    "expect_table_row_count_to_be_between": {
        "required": [],
        "optional": ["min_value", "max_value"],
        "param_types": {
            "min_value": (int, type(None)),
            "max_value": (int, type(None)),
        },
    },
    "expect_table_row_count_to_equal": {
        "required": ["value"],
        "optional": [],
        "param_types": {
            "value": int,
        },
    },
    # Null Expectations
    "expect_column_values_to_not_be_null": {
        "required": ["column"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "mostly": (int, float),
        },
    },
    "expect_column_values_to_be_null": {
        "required": ["column"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "mostly": (int, float),
        },
    },
    # Unique Expectations
    "expect_column_values_to_be_unique": {
        "required": ["column"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "mostly": (int, float),
        },
    },
    # Range Expectations
    "expect_column_values_to_be_between": {
        "required": ["column"],
        "optional": ["min_value", "max_value", "mostly", "parse_strings_as_datetimes"],
        "param_types": {
            "column": str,
            "min_value": (int, float, type(None)),
            "max_value": (int, float, type(None)),
            "mostly": (int, float),
            "parse_strings_as_datetimes": bool,
        },
    },
    "expect_column_values_to_be_in_set": {
        "required": ["column", "value_set"],
        "optional": ["mostly", "parse_strings_as_datetimes"],
        "param_types": {
            "column": str,
            "value_set": list,
            "mostly": (int, float),
            "parse_strings_as_datetimes": bool,
        },
    },
    "expect_column_values_to_be_in_type_list": {
        "required": ["column", "type_list"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "type_list": list,
            "mostly": (int, float),
        },
    },
    # String Expectations
    "expect_column_values_to_match_regex": {
        "required": ["column", "regex"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "regex": str,
            "mostly": (int, float),
        },
    },
    "expect_column_values_to_match_like_pattern": {
        "required": ["column", "like_pattern"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "like_pattern": str,
            "mostly": (int, float),
        },
    },
    "expect_column_values_to_not_match_regex": {
        "required": ["column", "regex"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "regex": str,
            "mostly": (int, float),
        },
    },
    # Date Expectations
    "expect_column_values_to_be_dateutil_parseable": {
        "required": ["column"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "mostly": (int, float),
        },
    },
    "expect_column_values_to_match_strftime_format": {
        "required": ["column", "strftime_format"],
        "optional": ["mostly"],
        "param_types": {
            "column": str,
            "strftime_format": str,
            "mostly": (int, float),
        },
    },
    # Statistical Expectations
    "expect_column_mean_to_be_between": {
        "required": ["column"],
        "optional": ["min_value", "max_value"],
        "param_types": {
            "column": str,
            "min_value": (int, float, type(None)),
            "max_value": (int, float, type(None)),
        },
    },
    "expect_column_median_to_be_between": {
        "required": ["column"],
        "optional": ["min_value", "max_value"],
        "param_types": {
            "column": str,
            "min_value": (int, float, type(None)),
            "max_value": (int, float, type(None)),
        },
    },
    "expect_column_stdev_to_be_between": {
        "required": ["column"],
        "optional": ["min_value", "max_value"],
        "param_types": {
            "column": str,
            "min_value": (int, float, type(None)),
            "max_value": (int, float, type(None)),
        },
    },
    # Set Operations
    "expect_column_values_to_not_be_in_set": {
        "required": ["column", "value_set"],
        "optional": ["mostly", "parse_strings_as_datetimes"],
        "param_types": {
            "column": str,
            "value_set": list,
            "mostly": (int, float),
            "parse_strings_as_datetimes": bool,
        },
    },
    # Comparison Expectations
    "expect_column_pair_values_A_to_be_greater_than_B": {
        "required": ["column_A", "column_B"],
        "optional": ["or_equal", "mostly"],
        "param_types": {
            "column_A": str,
            "column_B": str,
            "or_equal": bool,
            "mostly": (int, float),
        },
    },
    "expect_column_pair_values_to_be_equal": {
        "required": ["column_A", "column_B"],
        "optional": ["mostly"],
        "param_types": {
            "column_A": str,
            "column_B": str,
            "mostly": (int, float),
        },
    },
    # Aggregate Expectations
    "expect_column_distinct_values_to_be_in_set": {
        "required": ["column", "value_set"],
        "optional": ["parse_strings_as_datetimes"],
        "param_types": {
            "column": str,
            "value_set": list,
            "parse_strings_as_datetimes": bool,
        },
    },
    "expect_column_distinct_values_to_contain_set": {
        "required": ["column", "value_set"],
        "optional": ["parse_strings_as_datetimes"],
        "param_types": {
            "column": str,
            "value_set": list,
            "parse_strings_as_datetimes": bool,
        },
    },
    "expect_column_proportion_of_unique_values_to_be_between": {
        "required": ["column"],
        "optional": ["min_value", "max_value"],
        "param_types": {
            "column": str,
            "min_value": (int, float, type(None)),
            "max_value": (int, float, type(None)),
        },
    },
}


def validate_expectation(exp: Dict) -> Tuple[bool, Optional[str]]:
    """
    Validate a single Great Expectations expectation.

    Args:
        exp: Expectation dictionary with 'expectation_type' and kwargs

    Returns:
        Tuple of (is_valid, error_message)
        - (True, None) if valid
        - (False, error_message) if invalid
    """
    # Check if expectation_type exists
    if "expectation_type" not in exp:
        return False, "Missing required field 'expectation_type'"

    exp_type = exp["expectation_type"]

    # Check if it's a known GE expectation
    if exp_type not in GE_BUILTIN_EXPECTATIONS:
        return False, f"Unknown expectation type: '{exp_type}'. Must be a Great Expectations built-in expectation."

    exp_def = GE_BUILTIN_EXPECTATIONS[exp_type]

    # Get kwargs (everything except expectation_type)
    kwargs = {k: v for k, v in exp.items() if k != "expectation_type"}

    # Check required parameters
    required = exp_def.get("required", [])
    for param in required:
        if param not in kwargs:
            return False, f"Missing required parameter '{param}' for expectation '{exp_type}'"

    # Validate parameter types
    param_types = exp_def.get("param_types", {})
    for param, value in kwargs.items():
        if param in param_types:
            expected_type = param_types[param]

            # Handle tuple of types (Union types)
            if isinstance(expected_type, tuple):
                if not isinstance(value, expected_type):
                    type_names = ", ".join(t.__name__ for t in expected_type)
                    return False, (
                        f"Parameter '{param}' must be one of types [{type_names}], "
                        f"got {type(value).__name__} for expectation '{exp_type}'"
                    )
            else:
                if not isinstance(value, expected_type):
                    return False, (
                        f"Parameter '{param}' must be {expected_type.__name__}, "
                        f"got {type(value).__name__} for expectation '{exp_type}'"
                    )

    # Check for unknown parameters
    all_allowed = set(exp_def.get("required", []) + exp_def.get("optional", []))
    unknown_params = set(kwargs.keys()) - all_allowed
    if unknown_params:
        return False, f"Unknown parameters for expectation '{exp_type}': {', '.join(sorted(unknown_params))}"

    # Additional semantic validations
    error = _validate_semantics(exp_type, kwargs)
    if error:
        return False, error

    return True, None


def _validate_semantics(exp_type: str, kwargs: Dict) -> Optional[str]:
    """
    Perform semantic validations specific to certain expectation types.

    Args:
        exp_type: Expectation type
        kwargs: Expectation kwargs

    Returns:
        Error message if validation fails, None otherwise
    """
    # Validate min/max_value consistency
    if "min_value" in kwargs and "max_value" in kwargs:
        min_val = kwargs["min_value"]
        max_val = kwargs["max_value"]
        if min_val is not None and max_val is not None and min_val > max_val:
            return f"min_value ({min_val}) cannot be greater than max_value ({max_val}) for expectation '{exp_type}'"

    # Validate mostly is between 0 and 1
    if "mostly" in kwargs:
        mostly = kwargs["mostly"]
        if mostly is not None and (mostly < 0 or mostly > 1):
            return f"'mostly' parameter must be between 0 and 1, got {mostly} for expectation '{exp_type}'"

    # Validate value_set is not empty
    if "value_set" in kwargs:
        value_set = kwargs["value_set"]
        if not value_set:
            return f"'value_set' cannot be empty for expectation '{exp_type}'"

    # Validate column_set is not empty
    if "column_set" in kwargs:
        column_set = kwargs["column_set"]
        if not column_set:
            return f"'column_set' cannot be empty for expectation '{exp_type}'"

    # Validate type_list is not empty
    if "type_list" in kwargs:
        type_list = kwargs["type_list"]
        if not type_list:
            return f"'type_list' cannot be empty for expectation '{exp_type}'"

    return None


def validate_rules_batch(rules: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate a batch of data quality rules.

    Args:
        rules: List of expectation dictionaries

    Returns:
        Tuple of (valid_rules, invalid_rules)
        - valid_rules: List of rules that passed validation
        - invalid_rules: List of dicts with 'rule' and 'error' keys
    """
    valid_rules = []
    invalid_rules = []

    for rule in rules:
        is_valid, error = validate_expectation(rule)
        if is_valid:
            valid_rules.append(rule)
        else:
            invalid_rules.append({
                "rule": rule,
                "error": error
            })

    return valid_rules, invalid_rules


def match_pattern(value: str, pattern: str) -> bool:
    """
    Match a value against a pattern with wildcard support.

    Supports:
    - * : matches any sequence of characters (including empty)
    - ? : matches any single character
    - Literal characters match themselves

    Args:
        value: String value to match
        pattern: Pattern with * and ? wildcards

    Returns:
        True if value matches pattern, False otherwise

    Examples:
        >>> match_pattern("table_name", "table_*")
        True
        >>> match_pattern("column_id", "column_??")
        True
        >>> match_pattern("user_email", "user_*")
        True
        >>> match_pattern("status", "state")
        False
    """
    # Convert glob pattern to regex
    # Escape regex special characters except * and ?
    regex_pattern = ""
    for char in pattern:
        if char == "*":
            regex_pattern += ".*"
        elif char == "?":
            regex_pattern += "."
        elif char in r"\.^$+?{}[]|()":
            regex_pattern += "\\" + char
        else:
            regex_pattern += char

    # Anchor the pattern
    regex_pattern = "^" + regex_pattern + "$"

    try:
        return bool(re.match(regex_pattern, value))
    except re.error:
        # If regex compilation fails, return False
        return False


def get_expectation_info(exp_type: str) -> Optional[Dict[str, Any]]:
    """
    Get information about a GE expectation type.

    Args:
        exp_type: Expectation type name

    Returns:
        Dictionary with expectation info or None if not found
    """
    return GE_BUILTIN_EXPECTATIONS.get(exp_type)


def list_available_expectations() -> List[str]:
    """
    List all available GE built-in expectation types.

    Returns:
        Sorted list of expectation type names
    """
    return sorted(GE_BUILTIN_EXPECTATIONS.keys())


def get_expectation_categories() -> Dict[str, List[str]]:
    """
    Group expectations by category for easier navigation.

    Returns:
        Dictionary mapping category names to lists of expectation types
    """
    return {
        "schema": [
            "expect_table_columns_to_match_set",
            "expect_table_column_count_to_equal",
        ],
        "row_count": [
            "expect_table_row_count_to_be_between",
            "expect_table_row_count_to_equal",
        ],
        "null": [
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_null",
        ],
        "unique": [
            "expect_column_values_to_be_unique",
        ],
        "range": [
            "expect_column_values_to_be_between",
            "expect_column_values_to_be_in_set",
            "expect_column_values_to_be_in_type_list",
        ],
        "string": [
            "expect_column_values_to_match_regex",
            "expect_column_values_to_match_like_pattern",
            "expect_column_values_to_not_match_regex",
        ],
        "date": [
            "expect_column_values_to_be_dateutil_parseable",
            "expect_column_values_to_match_strftime_format",
        ],
        "statistical": [
            "expect_column_mean_to_be_between",
            "expect_column_median_to_be_between",
            "expect_column_stdev_to_be_between",
        ],
        "set_operations": [
            "expect_column_values_to_not_be_in_set",
        ],
        "comparison": [
            "expect_column_pair_values_A_to_be_greater_than_B",
            "expect_column_pair_values_to_be_equal",
        ],
        "aggregate": [
            "expect_column_distinct_values_to_be_in_set",
            "expect_column_distinct_values_to_contain_set",
            "expect_column_proportion_of_unique_values_to_be_between",
        ],
    }


if __name__ == "__main__":
    # Demo/validation script
    print("=== GE Validation Module Demo ===\n")

    # Test valid expectations
    print("1. Validating correct expectations:")
    valid_exps = [
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
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "min_value": 100,
            "max_value": 10000,
        },
    ]

    for exp in valid_exps:
        is_valid, error = validate_expectation(exp)
        print(f"  {exp['expectation_type']}: {'✓ Valid' if is_valid else f'✗ {error}'}")

    print("\n2. Validating incorrect expectations:")
    invalid_exps = [
        {"expectation_type": "expect_column_values_to_not_be_null"},  # missing column
        {"expectation_type": "expect_column_values_to_be_between", "column": "age"},  # missing min/max
        {"expectation_type": "expect_column_values_to_be_in_set", "column": "status"},  # missing value_set
        {"expectation_type": "unknown_expectation"},  # unknown type
        {"expectation_type": "expect_column_values_to_be_between", "column": 123},  # wrong type
    ]

    for exp in invalid_exps:
        is_valid, error = validate_expectation(exp)
        print(f"  {exp.get('expectation_type', 'NO_TYPE')}: {'✓ Valid' if is_valid else f'✗ {error}'}")

    print("\n3. Testing pattern matching:")
    patterns = [
        ("table_users", "table_*"),
        ("column_id", "column_??"),
        ("user_email", "user_*"),
        ("status", "state"),
    ]
    for value, pattern in patterns:
        result = match_pattern(value, pattern)
        print(f"  '{value}' vs '{pattern}': {'✓ Match' if result else '✗ No match'}")

    print("\n4. Available expectation categories:")
    categories = get_expectation_categories()
    for cat, exps in categories.items():
        print(f"  {cat}: {len(exps)} expectations")

    print("\n5. Total available expectations:", len(list_available_expectations()))
