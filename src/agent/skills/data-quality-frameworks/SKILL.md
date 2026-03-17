---
name: data-quality-frameworks
description: Implement data quality validation with Great Expectations. Use when building data quality pipelines, implementing validation rules, or establishing data contracts. ALWAYS use the provided scripts instead of generating JSON or Python code directly.
---

# Data Quality Frameworks

Production patterns for implementing data quality with Great Expectations, dbt tests, and data contracts to ensure reliable data pipelines.

## ⚠️ CRITICAL: Use Scripts First

**AmandaX MUST use the provided scripts for ALL data quality operations.**

```
❌ NEVER: Generate JSON files directly
❌ NEVER: Run `great_expectations init` directly
❌ NEVER: Write Python code for expectation suites
❌ NEVER: Skip the scripts and do things manually

✅ ALWAYS: Use scripts in .amandax/skills/data-quality-frameworks/scripts/
✅ ALWAYS: Call scripts via execute() tool
✅ ALWAYS: Parse JSON output from scripts
✅ ALWAYS: Report script results to user
```

### Why Scripts?

The scripts provide:
- **Validation**: Rules are validated against GE built-in expectations
- **Consistency**: All rules follow the same format and structure
- **Traceability**: Each rule gets a unique rule_id
- **Safety**: Prevents malformed rules from breaking validation

### Script Location

```
.amandax/skills/data-quality-frameworks/scripts/
├── config_ge.py        # Initialize/check GE environment
├── upsert_rules.py     # Add or update rules (with validation)
├── list_rules.py       # List existing rules
├── delete_rules.py     # Delete rules
├── run_suite.py        # Run validation
├── validate_rules.py   # Validate existing rules for errors
├── to_dbt.py           # Export to dbt format
└── to_sql.py           # Export to SQL
```

---

## Quick Start (AmandaX Workflow)

When asked to add data quality rules, follow this workflow:

### Step 1: Check GE Environment

```bash
python .amandax/skills/data-quality-frameworks/scripts/config_ge.py --status
```

**If `status: "not_initialized"`**, initialize:

```bash
python .amandax/skills/data-quality-frameworks/scripts/config_ge.py --init
```

### Step 2: Add Rules (with validation)

```bash
python .amandax/skills/data-quality-frameworks/scripts/upsert_rules.py \
  --table orders \
  --rules '[
    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
    {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
    {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "status", "value_set": ["pending", "shipped", "delivered"]}}
  ]'
```

### Step 3: List Rules (verify)

```bash
python .amandax/skills/data-quality-frameworks/scripts/list_rules.py --table orders
```

### Step 4: Run Validation

```bash
python .amandax/skills/data-quality-frameworks/scripts/run_suite.py --table orders
```

### Step 5: Validate Rules (optional but recommended)

```bash
python .amandax/skills/data-quality-frameworks/scripts/validate_rules.py --table orders
```

---

## Script Reference

| Script | Purpose | Key Parameters | Example |
|--------|---------|----------------|---------|
| `config_ge.py` | Initialize/check GE environment | `--init`, `--status`, `--reset` | `python config_ge.py --status` |
| `upsert_rules.py` | Add/update rules with validation | `--table`, `--rules` (JSON), `--file`, `--dry-run` | `python upsert_rules.py --table orders --rules '[...]'` |
| `list_rules.py` | List rules with filtering | `--table`, `--type`, `--column`, `--severity`, `--all` | `python list_rules.py --table orders` |
| `delete_rules.py` | Delete rules | `--table`, `--rule-id`, `--type`, `--column`, `--all` | `python delete_rules.py --table orders --rule-id r_abc123` |
| `run_suite.py` | Run validation | `--table`, `--type`, `--severity`, `--report`, `--fail-on-error` | `python run_suite.py --table orders` |
| `validate_rules.py` | Validate existing rules | `--table`, `--all`, `--fix`, `--verbose` | `python validate_rules.py --table orders` |
| `to_dbt.py` | Export to dbt format | `--table`, `--output` | `python to_dbt.py --table orders --output models/` |
| `to_sql.py` | Export to SQL | `--table`, `--output` | `python to_sql.py --table orders --output checks/` |

### Pattern Matching

All scripts support wildcards:
- `*` matches any sequence of characters
- `?` matches a single character

```bash
# Match all tables ending with "_orders"
python list_rules.py --table "*_orders"

# Match all columns ending with "_id"
python list_rules.py --column "*_id"

# Match types containing "unique"
python list_rules.py --type "*unique*"
```

---

## Script Output Formats

All scripts output JSON for easy parsing:

### config_ge.py --status

```json
{"status": "initialized", "ge_dir": "artifacts/great_expectations", "config_exists": true, "suites_count": 3, "suites": ["orders_suite", "customers_suite", "products_suite"]}
```

### upsert_rules.py

```json
{
  "success": true,
  "table": "orders",
  "suite_name": "orders_suite",
  "suite_path": "artifacts/great_expectations/expectations/orders_suite.json",
  "added": 2,
  "updated": 1,
  "total_expectations": 5,
  "added_rules": [{"rule_id": "r_abc123", "type": "expect_column_values_to_not_be_null", "column": "order_id", "severity": "error"}],
  "updated_rules": [...]
}
```

### list_rules.py

```json
{
  "success": true,
  "table": "orders",
  "suite_path": "artifacts/great_expectations/expectations/orders_suite.json",
  "total_rules": 5,
  "rules": [
    {"rule_id": "r_abc123", "expectation_type": "expect_column_values_to_not_be_null", "column": "order_id", "severity": "error"},
    ...
  ]
}
```

### validate_rules.py

```json
{
  "success": false,
  "tables_checked": 3,
  "total_rules": 15,
  "valid_rules": 14,
  "invalid_rules": 1,
  "error_count": 1,
  "warning_count": 2,
  "issues": [
    {
      "table": "orders",
      "rule_index": 2,
      "expectation_type": "invalid_expectation_type",
      "column": "status",
      "issue": "Unknown expectation type: 'invalid_expectation_type'",
      "severity": "error"
    }
  ]
}
```

---

## GE Built-in Expectations

When adding rules, ONLY use these GE built-in expectation types:

### Schema Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_table_columns_to_match_set` | `column_set` | Column structure |
| `expect_table_column_count_to_equal` | `value` | Column count |

### Row Count Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_table_row_count_to_be_between` | `min_value`, `max_value` | Row count range |
| `expect_table_row_count_to_equal` | `value` | Exact row count |

### Null Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_values_to_not_be_null` | `column` | No nulls |
| `expect_column_values_to_be_null` | `column` | Should be null |

### Unique Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_values_to_be_unique` | `column` | Primary key |

### Range Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_values_to_be_between` | `column` | Numeric/date range |
| `expect_column_values_to_be_in_set` | `column`, `value_set` | Enum values |
| `expect_column_values_to_be_in_type_list` | `column`, `type_list` | Data types |

### String Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_values_to_match_regex` | `column`, `regex` | Pattern match |
| `expect_column_values_to_match_like_pattern` | `column`, `like_pattern` | SQL LIKE |
| `expect_column_values_to_not_match_regex` | `column`, `regex` | Negative pattern |

### Date Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_values_to_be_dateutil_parseable` | `column` | Valid date |
| `expect_column_values_to_match_strftime_format` | `column`, `strftime_format` | Date format |

### Statistical Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_mean_to_be_between` | `column` | Mean range |
| `expect_column_median_to_be_between` | `column` | Median range |
| `expect_column_stdev_to_be_between` | `column` | Std dev range |

### Comparison Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_pair_values_A_to_be_greater_than_B` | `column_A`, `column_B` | Column comparison |
| `expect_column_pair_values_to_be_equal` | `column_A`, `column_B` | Columns equal |

### Aggregate Expectations
| Type | Required Params | Use Case |
|------|----------------|----------|
| `expect_column_distinct_values_to_be_in_set` | `column`, `value_set` | Distinct values |
| `expect_column_distinct_values_to_contain_set` | `column`, `value_set` | Contains values |
| `expect_column_proportion_of_unique_values_to_be_between` | `column` | Uniqueness ratio |

---

## Data Quality Dimensions

| Dimension | Description | GE Expectation Type |
|-----------|-------------|---------------------|
| **Completeness** | No missing values | `expect_column_values_to_not_be_null` |
| **Uniqueness** | No duplicates | `expect_column_values_to_be_unique` |
| **Validity** | Values in expected range | `expect_column_values_to_be_in_set` |
| **Accuracy** | Data matches reality | Cross-reference validation |
| **Consistency** | No contradictions | `expect_column_pair_values_A_to_be_greater_than_B` |
| **Timeliness** | Data is recent | `expect_column_max_to_be_between` |

---

## Common Workflows

### Workflow 1: Add Rules for a New Table

```bash
# 1. Check environment
python .amandax/skills/data-quality-frameworks/scripts/config_ge.py --status

# 2. Add rules (upsert validates automatically)
python .amandax/skills/data-quality-frameworks/scripts/upsert_rules.py --table orders --rules '[
  {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "order_id"}},
  {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}},
  {"expectation_type": "expect_column_values_to_be_in_set", "kwargs": {"column": "status", "value_set": ["pending", "shipped", "delivered", "cancelled"]}}
]'

# 3. Verify rules
python .amandax/skills/data-quality-frameworks/scripts/list_rules.py --table orders

# 4. Run validation
python .amandax/skills/data-quality-frameworks/scripts/run_suite.py --table orders
```

### Workflow 2: Fix Invalid Rules

```bash
# 1. Validate all rules
python .amandax/skills/data-quality-frameworks/scripts/validate_rules.py --all --verbose

# 2. Auto-fix (removes invalid rules)
python .amandax/skills/data-quality-frameworks/scripts/validate_rules.py --table orders --fix

# 3. Re-add correct rules
python .amandax/skills/data-quality-frameworks/scripts/upsert_rules.py --table orders --rules '[...]'
```

### Workflow 3: Clean Up Rules

```bash
# 1. List all rules for a table
python .amandax/skills/data-quality-frameworks/scripts/list_rules.py --table orders

# 2. Delete specific rules
python .amandax/skills/data-quality-frameworks/scripts/delete_rules.py --table orders --rule-id r_abc123

# 3. Or delete by pattern
python .amandax/skills/data-quality-frameworks/scripts/delete_rules.py --table orders --type "*null*"

# 4. Or delete all rules for a table
python .amandax/skills/data-quality-frameworks/scripts/delete_rules.py --table orders --all
```

---

## Best Practices

### Do's

- **Use scripts first** - Never generate JSON or Python code directly
- **Validate rules** - Run `validate_rules.py` after manual edits
- **Test early** - Validate source data before transformations
- **Document expectations** - Add descriptions in meta fields
- **Alert on failures** - Integrate with monitoring

### Don'ts

- **Don't skip scripts** - They provide validation and consistency
- **Don't use unknown expectations** - Only GE built-in types are supported
- **Don't ignore validation errors** - Fix them before running suites
- **Don't test everything** - Focus on critical columns
- **Don't hardcode thresholds** - Use dynamic baselines

---

## Advanced Patterns

### Pattern: dbt Data Tests

```yaml
# models/marts/core/_core__models.yml
version: 2

models:
  - name: fct_orders
    description: Order fact table
    tests:
      - dbt_utils.recency:
          datepart: day
          field: created_at
          interval: 1
      - dbt_utils.at_least_one
      - dbt_utils.expression_is_true:
          expression: "total_amount >= 0"

    columns:
      - name: order_id
        tests:
          - unique
          - not_null

      - name: status
        tests:
          - accepted_values:
              values: ["pending", "processing", "shipped", "delivered", "cancelled"]
```

### Pattern: Data Contracts

```yaml
# contracts/orders_contract.yaml
apiVersion: datacontract.com/v1.0.0
kind: DataContract
metadata:
  name: orders
  version: 1.0.0

schema:
  type: object
  properties:
    order_id:
      type: string
      format: uuid
      required: true
      unique: true

    status:
      type: string
      enum: [pending, processing, shipped, delivered, cancelled]

quality:
  type: SodaCL
  specification:
    checks for orders:
      - row_count > 0
      - missing_count(order_id) = 0
      - duplicate_count(order_id) = 0
      - freshness(created_at) < 24h
```

---

## When to Use This Skill

- Implementing data quality checks in pipelines
- Setting up Great Expectations validation
- Building comprehensive dbt test suites
- Establishing data contracts between teams
- Monitoring data quality metrics
- Automating data validation in CI/CD
