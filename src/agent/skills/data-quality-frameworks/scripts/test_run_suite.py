#!/usr/bin/env python3
"""
Tests for run_suite.py - data quality suite execution with filtering.

Tests cover:
- Pattern matching for expectation_type
- Severity filtering (exact match)
- Filter combinations
- Report generation
- CI mode (fail-on-error)
- Output format validation
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest

# Import module under test
sys.path.insert(0, str(Path(__file__).parent))
from run_suite import (
    filter_expectations,
    run_suite,
    generate_report,
)


class TestFilterExpectations:
    """Test expectation filtering by type pattern and severity."""

    @pytest.fixture
    def sample_expectations(self):
        """Sample expectations with various types and severities."""
        return [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "user_id"},
                "meta": {"rule_id": "r_001", "severity": "error"},
            },
            {
                "expectation_type": "expect_column_values_to_be_null",
                "kwargs": {"column": "deleted_at"},
                "meta": {"rule_id": "r_002", "severity": "warning"},
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "email"},
                "meta": {"rule_id": "r_003", "severity": "error"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 0, "max_value": 120},
                "meta": {"rule_id": "r_004", "severity": "info"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "status", "value_set": ["active", "inactive"]},
                "meta": {"rule_id": "r_005", "severity": "warning"},
            },
        ]

    def test_no_filters_returns_all(self, sample_expectations):
        """Test that no filters returns all expectations."""
        result = filter_expectations(sample_expectations)
        assert len(result) == 5
        assert result == sample_expectations

    def test_type_pattern_wildcard_prefix(self, sample_expectations):
        """Test type pattern with wildcard prefix (*_null)."""
        result = filter_expectations(sample_expectations, type_pattern="*_null")
        assert len(result) == 2
        types = [e["expectation_type"] for e in result]
        assert "expect_column_values_to_not_be_null" in types
        assert "expect_column_values_to_be_null" in types

    def test_type_pattern_wildcard_suffix(self, sample_expectations):
        """Test type pattern with wildcard suffix (expect_column_*)."""
        result = filter_expectations(sample_expectations, type_pattern="expect_column_*")
        assert len(result) == 5  # All start with expect_column_

    def test_type_pattern_wildcard_middle(self, sample_expectations):
        """Test type pattern with wildcard in middle (*_be_*)."""
        result = filter_expectations(sample_expectations, type_pattern="*_be_*")
        # Should match: to_not_be_null, to_be_null, to_be_unique, to_be_between, to_be_in_set
        assert len(result) == 5

    def test_type_pattern_question_mark(self, sample_expectations):
        """Test type pattern with ? wildcard for single character."""
        # Create a custom expectation for this test
        custom_exps = [
            {"expectation_type": "exp_1", "kwargs": {}, "meta": {}},
            {"expectation_type": "exp_2", "kwargs": {}, "meta": {}},
            {"expectation_type": "exp_12", "kwargs": {}, "meta": {}},
        ]
        result = filter_expectations(custom_exps, type_pattern="exp_?")
        assert len(result) == 2
        types = [e["expectation_type"] for e in result]
        assert "exp_1" in types
        assert "exp_2" in types

    def test_type_pattern_exact_match(self, sample_expectations):
        """Test exact type pattern match (no wildcards)."""
        result = filter_expectations(
            sample_expectations,
            type_pattern="expect_column_values_to_be_unique"
        )
        assert len(result) == 1
        assert result[0]["expectation_type"] == "expect_column_values_to_be_unique"

    def test_type_pattern_no_match(self, sample_expectations):
        """Test type pattern that matches nothing."""
        result = filter_expectations(sample_expectations, type_pattern="*nonexistent*")
        assert len(result) == 0

    def test_severity_filter_error(self, sample_expectations):
        """Test severity filter for 'error' (exact match)."""
        result = filter_expectations(sample_expectations, severity="error")
        assert len(result) == 2
        for exp in result:
            assert exp["meta"]["severity"] == "error"

    def test_severity_filter_warning(self, sample_expectations):
        """Test severity filter for 'warning' (exact match)."""
        result = filter_expectations(sample_expectations, severity="warning")
        assert len(result) == 2
        for exp in result:
            assert exp["meta"]["severity"] == "warning"

    def test_severity_filter_info(self, sample_expectations):
        """Test severity filter for 'info' (exact match)."""
        result = filter_expectations(sample_expectations, severity="info")
        assert len(result) == 1
        assert result[0]["meta"]["severity"] == "info"

    def test_severity_default_is_error(self):
        """Test that expectations without severity default to 'error'."""
        exps = [
            {"expectation_type": "exp1", "kwargs": {}, "meta": {}},  # No severity
            {"expectation_type": "exp2", "kwargs": {}, "meta": {"severity": "warning"}},
        ]
        result = filter_expectations(exps, severity="error")
        assert len(result) == 1
        assert result[0]["expectation_type"] == "exp1"

    def test_combined_type_and_severity_filters(self, sample_expectations):
        """Test combining type pattern and severity filters."""
        result = filter_expectations(
            sample_expectations,
            type_pattern="*_null",
            severity="error"
        )
        assert len(result) == 1
        assert result[0]["expectation_type"] == "expect_column_values_to_not_be_null"
        assert result[0]["meta"]["severity"] == "error"

    def test_combined_filters_no_match(self, sample_expectations):
        """Test combined filters that match nothing."""
        result = filter_expectations(
            sample_expectations,
            type_pattern="*_unique",
            severity="info"  # unique rule has severity=error
        )
        assert len(result) == 0


class TestRunSuite:
    """Test run_suite function with mocked dependencies."""

    @pytest.fixture
    def sample_suite(self):
        """Sample GE suite for testing."""
        return {
            "data_asset_type": None,
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "order_id"},
                    "meta": {"rule_id": "r_001", "severity": "error"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "order_id"},
                    "meta": {"rule_id": "r_002", "severity": "error"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {"column": "amount", "min_value": 0},
                    "meta": {"rule_id": "r_003", "severity": "warning"},
                },
            ],
            "meta": {"great_expectations_version": "0.18.0"},
        }

    @patch("run_suite.load_settings")
    @patch("run_suite.get_db_connection")
    @patch("run_suite.run_validation")
    def test_run_suite_with_type_filter(
        self, mock_validate, mock_db, mock_settings, sample_suite, tmp_path
    ):
        """Test run_suite applies type pattern filter."""
        # Create actual suite file
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        # Patch GE_DIR
        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            mock_settings.return_value = {}
            mock_db.return_value = ("postgresql", "postgresql://...")
            mock_validate.return_value = {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 1,
                    "successful_expectations": 1,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "failures": [],
            }

            result = run_suite(table="orders", type_pattern="*_null*")

            # Verify filter was applied (only 1 expectation passed to validation)
            assert mock_validate.called
            call_args = mock_validate.call_args
            assert call_args is not None
            # run_validation(db_type, conn_string, table, expectations)
            expectations_passed = call_args[0][3]  # Fourth argument is expectations list
            assert len(expectations_passed) == 1
            assert expectations_passed[0]["expectation_type"] == "expect_column_values_to_not_be_null"

    @patch("run_suite.load_settings")
    @patch("run_suite.get_db_connection")
    @patch("run_suite.run_validation")
    def test_run_suite_with_severity_filter(
        self, mock_validate, mock_db, mock_settings, sample_suite, tmp_path
    ):
        """Test run_suite applies severity filter."""
        # Create actual suite file
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            mock_settings.return_value = {}
            mock_db.return_value = ("postgresql", "postgresql://...")
            mock_validate.return_value = {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 2,
                    "successful_expectations": 2,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "failures": [],
            }

            result = run_suite(table="orders", severity="error")

            # Verify filter was applied (2 error expectations)
            assert mock_validate.called
            call_args = mock_validate.call_args
            assert call_args is not None
            expectations_passed = call_args[0][3]
            assert len(expectations_passed) == 2
            for exp in expectations_passed:
                assert exp["meta"]["severity"] == "error"

    @patch("run_suite.load_settings")
    @patch("run_suite.get_db_connection")
    @patch("run_suite.run_validation")
    def test_run_suite_output_includes_filters(
        self, mock_validate, mock_db, mock_settings, sample_suite, tmp_path
    ):
        """Test that output includes filters section."""
        # Create actual suite file
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            mock_settings.return_value = {}
            mock_db.return_value = ("postgresql", "postgresql://...")
            mock_validate.return_value = {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 1,
                    "successful_expectations": 1,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "failures": [],
            }

            result = run_suite(
                table="orders",
                type_pattern="*_null*",
                severity="error"
            )

            assert "filters" in result
            assert result["filters"]["type"] == "*_null*"
            assert result["filters"]["severity"] == "error"

    def test_run_suite_no_matches_returns_warning(self, sample_suite, tmp_path):
        """Test run_suite returns warning when no expectations match filters."""
        # Create actual suite file
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            result = run_suite(
                table="orders",
                type_pattern="*nonexistent*"
            )

            assert result["success"] is True
            assert result["statistics"]["evaluated_expectations"] == 0
            assert "warning" in result
            assert result["warning"] == "No expectations matched the filters"

    def test_run_suite_missing_table(self):
        """Test run_suite raises error when table is missing."""
        with pytest.raises(ValueError, match="Table name required"):
            run_suite(table=None)

    def test_run_suite_suite_not_found(self, tmp_path):
        """Test run_suite returns error when suite not found."""
        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            result = run_suite(table="nonexistent")
            assert "error" in result
            assert "Suite not found" in result["error"]


class TestReportGeneration:
    """Test report generation for HTML and JSON formats."""

    @pytest.fixture
    def sample_result(self):
        """Sample validation result for report generation."""
        return {
            "success": False,
            "table": "orders",
            "run_time": "2026-03-16T12:00:00",
            "filters": {
                "type": "*_null*",
                "severity": "error",
            },
            "statistics": {
                "evaluated_expectations": 5,
                "successful_expectations": 4,
                "unsuccessful_expectations": 1,
                "success_percent": 80.0,
            },
            "failures": [
                {
                    "rule_id": "r_001",
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "column": "user_id",
                    "severity": "error",
                    "unexpected_count": 10,
                    "unexpected_percent": 2.5,
                }
            ],
        }

    def test_generate_html_report(self, sample_result):
        """Test HTML report generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "report.html"
            generate_report(sample_result, "html", str(output_path))

            assert output_path.exists()
            content = output_path.read_text()

            # Verify key elements
            assert "Data Quality Report: orders" in content
            assert "2026-03-16T12:00:00" in content
            assert "FAILED" in content  # Because success=False
            assert "80.0%" in content
            assert "r_001" in content
            assert "expect_column_values_to_not_be_null" in content
            assert "user_id" in content
            assert "error" in content.lower()
            assert "Filters Applied" in content

    def test_generate_html_report_success(self):
        """Test HTML report generation for successful validation."""
        result = {
            "success": True,
            "table": "users",
            "run_time": "2026-03-16T12:00:00",
            "filters": {},
            "statistics": {
                "evaluated_expectations": 3,
                "successful_expectations": 3,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            "failures": [],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "report.html"
            generate_report(result, "html", str(output_path))

            content = output_path.read_text()
            assert "PASSED" in content
            assert "100.0%" in content

    def test_generate_html_report_no_filters(self, sample_result):
        """Test HTML report without filters section."""
        sample_result["filters"] = {}

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "report.html"
            generate_report(sample_result, "html", str(output_path))

            content = output_path.read_text()
            assert "Filters Applied" not in content

    def test_generate_json_report(self, sample_result):
        """Test JSON report generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "report.json"
            generate_report(sample_result, "json", str(output_path))

            assert output_path.exists()

            with open(output_path) as f:
                loaded = json.load(f)

            assert loaded == sample_result

    def test_generate_report_creates_parent_dirs(self, sample_result):
        """Test that generate_report creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "nested" / "deep" / "report.html"
            generate_report(sample_result, "html", str(output_path))

            assert output_path.exists()


class TestCIMode:
    """Test CI mode (fail-on-error) behavior."""

    @patch("run_suite.load_settings")
    @patch("run_suite.get_db_connection")
    @patch("run_suite.run_validation")
    def test_fail_on_error_exits_on_failure(
        self, mock_validate, mock_db, mock_settings, tmp_path
    ):
        """Test that fail_on_error exits with code 1 on validation failure."""
        # Create actual suite file
        sample_suite = {
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                    "meta": {"rule_id": "r_001", "severity": "error"},
                }
            ],
        }
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            mock_settings.return_value = {}
            mock_db.return_value = ("postgresql", "postgresql://...")
            mock_validate.return_value = {
                "success": False,
                "statistics": {
                    "evaluated_expectations": 1,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 1,
                    "success_percent": 0.0,
                },
                "failures": [{"rule_id": "r_001", "error": "validation failed"}],
            }

            with pytest.raises(SystemExit) as exc_info:
                run_suite(table="orders", fail_on_error=True)

            assert exc_info.value.code == 1

    @patch("run_suite.load_settings")
    @patch("run_suite.get_db_connection")
    @patch("run_suite.run_validation")
    def test_fail_on_error_no_exit_on_success(
        self, mock_validate, mock_db, mock_settings, tmp_path
    ):
        """Test that fail_on_error doesn't exit when validation succeeds."""
        # Create actual suite file
        sample_suite = {
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                    "meta": {"rule_id": "r_001", "severity": "error"},
                }
            ],
        }
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            mock_settings.return_value = {}
            mock_db.return_value = ("postgresql", "postgresql://...")
            mock_validate.return_value = {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 1,
                    "successful_expectations": 1,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0,
                },
                "failures": [],
            }

            # Should not raise
            result = run_suite(table="orders", fail_on_error=True)
            assert result["success"] is True


class TestOutputFormat:
    """Test output format validation."""

    @patch("run_suite.load_settings")
    @patch("run_suite.get_db_connection")
    @patch("run_suite.run_validation")
    def test_output_format_success(
        self, mock_validate, mock_db, mock_settings, tmp_path
    ):
        """Test successful output format matches specification."""
        # Create actual suite file
        sample_suite = {
            "expectation_suite_name": "orders_suite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                    "meta": {"rule_id": "r_001", "severity": "error"},
                }
            ],
        }
        ge_dir = tmp_path / "artifacts" / "great_expectations" / "expectations"
        ge_dir.mkdir(parents=True)
        suite_file = ge_dir / "orders_suite.json"
        suite_file.write_text(json.dumps(sample_suite))

        with patch("run_suite.GE_DIR", tmp_path / "artifacts" / "great_expectations"):
            mock_settings.return_value = {}
            mock_db.return_value = ("postgresql", "postgresql://...")
            mock_validate.return_value = {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 5,
                    "successful_expectations": 4,
                    "unsuccessful_expectations": 1,
                    "success_percent": 80.0,
                },
                "failures": [],
            }

            result = run_suite(
                table="orders",
                type_pattern="*null*",
                severity="error"
            )

            # Verify output structure
            assert result["success"] is True
            assert result["table"] == "orders"
            assert "run_time" in result
            assert "filters" in result
            assert result["filters"]["type"] == "*null*"
            assert result["filters"]["severity"] == "error"
            assert "statistics" in result
            assert result["statistics"]["evaluated_expectations"] == 5
            assert result["statistics"]["successful_expectations"] == 4
            assert result["statistics"]["unsuccessful_expectations"] == 1
            assert result["statistics"]["success_percent"] == 80.0
            assert "failures" in result
            assert isinstance(result["failures"], list)


class TestIdentifierValidation:
    """Test identifier validation for table and column names."""

    def test_is_safe_identifier_valid_names(self):
        """Test that valid identifiers pass validation."""
        from run_suite import is_safe_identifier

        # Valid identifiers
        assert is_safe_identifier("users") is True
        assert is_safe_identifier("order_items") is True
        assert is_safe_identifier("table123") is True
        assert is_safe_identifier("_private") is True
        assert is_safe_identifier("schema.table") is True  # Schema-qualified

    def test_is_safe_identifier_invalid_names(self):
        """Test that invalid identifiers fail validation."""
        from run_suite import is_safe_identifier

        # Invalid identifiers (SQL injection attempts)
        assert is_safe_identifier("") is False  # Empty
        assert is_safe_identifier("users; DROP TABLE orders;") is False  # SQL injection
        assert is_safe_identifier("users--comment") is False  # SQL comment
        assert is_safe_identifier("users' OR '1'='1") is False  # SQL injection
        assert is_safe_identifier("users UNION SELECT *") is False  # SQL injection
        assert is_safe_identifier("123table") is False  # Starts with number
        assert is_safe_identifier("user-name") is False  # Hyphen not allowed
        assert is_safe_identifier("user name") is False  # Space not allowed

    def test_run_validation_rejects_invalid_table_name(self):
        """Test that run_validation rejects malicious table names."""
        from run_suite import run_validation

        result = run_validation(
            db_type="postgresql",
            conn_string="postgresql://test",
            table="users; DROP TABLE orders;",
            expectations=[]
        )

        assert "error" in result
        assert "Invalid table name" in result["error"]

    def test_run_validation_rejects_invalid_column_name(self):
        """Test that run_validation rejects malicious column names in expectations."""
        from run_suite import run_validation

        mock_engine = MagicMock()
        mock_conn = MagicMock()

        # Mock the count query result
        mock_result = MagicMock()
        mock_result.scalar.return_value = 100  # row count

        def capture_execute(query, params=None):
            return mock_result

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute = capture_execute

        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            mock_engine.connect.return_value = mock_conn

            expectations = [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id; DROP TABLE users;"},
                    "meta": {"rule_id": "r_001"},
                }
            ]

            result = run_validation(
                db_type="postgresql",
                conn_string="postgresql://test",
                table="users",
                expectations=expectations
            )

            # Should have at least one failure due to invalid column
            assert result["statistics"]["unsuccessful_expectations"] >= 1
            # The error should mention invalid column
            assert any("Invalid column name" in str(f) for f in result["failures"])


class TestParameterizedQueries:
    """Test that numeric values use parameterized queries."""

    def test_between_values_parameterized(self):
        """Test that min/max values in expect_column_values_to_be_between use parameters."""
        from run_suite import run_validation

        mock_engine = MagicMock()
        mock_conn = MagicMock()

        captured_params = []

        def capture_execute(query, params=None):
            if params:
                captured_params.append(params.copy())
            mock_result = MagicMock()
            mock_result.scalar.return_value = 0
            return mock_result

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute = capture_execute

        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            mock_engine.connect.return_value = mock_conn

            expectations = [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "age",
                        "min_value": 0,
                        "max_value": 120,
                    },
                    "meta": {"rule_id": "r_001"},
                }
            ]

            result = run_validation(
                db_type="postgresql",
                conn_string="postgresql://test",
                table="users",
                expectations=expectations,
            )

            # Verify parameters were passed for min/max values
            assert len(captured_params) >= 1
            params = captured_params[0]

            # The values should be passed as parameters, not interpolated
            assert "min_val" in params or "max_val" in params
            assert params.get("min_val") == 0 or params.get("max_val") == 120

    def test_between_values_sql_injection_prevented(self):
        """Test that SQL injection via min/max values is prevented."""
        from run_suite import run_validation

        mock_engine = MagicMock()
        mock_conn = MagicMock()

        captured_queries = []
        captured_params = []

        def capture_execute(query, params=None):
            captured_queries.append(str(query))
            if params:
                captured_params.append(params.copy())
            mock_result = MagicMock()
            mock_result.scalar.return_value = 0
            return mock_result

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute = capture_execute

        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            mock_engine.connect.return_value = mock_conn

            # Attempt SQL injection via min_value (should be converted or rejected)
            expectations = [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": "age",
                        "min_value": "0 OR 1=1",  # String injection attempt
                        "max_value": 120,
                    },
                    "meta": {"rule_id": "r_001"},
                }
            ]

            result = run_validation(
                db_type="postgresql",
                conn_string="postgresql://test",
                table="users",
                expectations=expectations,
            )

            # Verify the injection string is not in the query directly
            query_str = captured_queries[0] if captured_queries else ""
            assert "OR 1=1" not in query_str


class TestSQLInjectionPrevention:
    """Test that value_set uses parameterized queries to prevent SQL injection."""

    def test_value_set_parameterized_query_safe(self):
        """Test that value_set values with SQL special chars are safely escaped."""
        # Import run_validation directly
        from run_suite import run_validation

        # Create mock engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()

        # Track the parameters passed to execute
        captured_params = []

        def capture_execute(query, params=None):
            # Capture params for verification
            if params:
                captured_params.append(params.copy())
            # Return mock result with 0 invalid count
            mock_result = MagicMock()
            mock_result.scalar.return_value = 0
            return mock_result

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute = capture_execute

        # Mock the engine.connect to return our mock connection
        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            mock_engine.connect.return_value = mock_conn

            # Test with values containing SQL injection attempts
            dangerous_values = [
                "'; DROP TABLE users; --",
                "value') OR ('1'='1",
                "normal_value",
            ]

            expectations = [
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "status",
                        "value_set": dangerous_values,
                    },
                    "meta": {"rule_id": "r_001", "severity": "error"},
                }
            ]

            result = run_validation(
                db_type="postgresql",
                conn_string="postgresql://test",
                table="test_table",
                expectations=expectations,
            )

            # Verify parameters were passed (not string interpolation)
            assert len(captured_params) == 1
            params = captured_params[0]

            # Verify the dangerous values are passed as parameters, not in SQL string
            assert "val_0" in params
            assert "val_1" in params
            assert "val_2" in params
            assert params["val_0"] == "'; DROP TABLE users; --"
            assert params["val_1"] == "value') OR ('1'='1"
            assert params["val_2"] == "normal_value"

            # Verify success (0 invalid count)
            assert result["success"] is True
            assert result["statistics"]["successful_expectations"] == 1

    def test_value_set_with_quotes_escaped(self):
        """Test that single quotes in values are properly handled."""
        from run_suite import run_validation

        mock_engine = MagicMock()
        mock_conn = MagicMock()

        captured_params = []

        def capture_execute(query, params=None):
            if params:
                captured_params.append(params.copy())
            mock_result = MagicMock()
            mock_result.scalar.return_value = 0
            return mock_result

        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute = capture_execute

        with patch("sqlalchemy.create_engine", return_value=mock_engine):
            mock_engine.connect.return_value = mock_conn

            # Values with various quote patterns
            values_with_quotes = [
                "it's",
                'he said "hello"',
                "O'Brien",
            ]

            expectations = [
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "name",
                        "value_set": values_with_quotes,
                    },
                    "meta": {"rule_id": "r_001"},
                }
            ]

            result = run_validation(
                db_type="postgresql",
                conn_string="postgresql://test",
                table="users",
                expectations=expectations,
            )

            # Verify parameters contain the original values with quotes
            params = captured_params[0]
            assert params["val_0"] == "it's"
            assert params["val_1"] == 'he said "hello"'
            assert params["val_2"] == "O'Brien"

            assert result["success"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
