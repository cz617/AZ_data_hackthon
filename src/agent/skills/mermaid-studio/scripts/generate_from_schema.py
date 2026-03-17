"""
Generate Mermaid diagrams from distributed schema structure.

Usage:
    # All tables (relationship diagram)
    python .amandax/skills/mermaid-studio/scripts/generate_from_schema.py --schema-dir artifacts/schemas/bird_testing --output artifacts/diagrams/diagram.md

    # Search by pattern (regex)
    python .amandax/skills/mermaid-studio/scripts/generate_from_schema.py --schema-dir artifacts/schemas/bird_testing --pattern "user.*" --output artifacts/diagrams/users.md

    # With column limit (recommended for large tables)
    python .amandax/skills/mermaid-studio/scripts/generate_from_schema.py --schema-dir artifacts/schemas/bird_testing --max-columns 10 --output artifacts/diagrams/diagram.md
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# =============================================================================
# Schema Utilities
# =============================================================================

RELATIONSHIP_STYLES = {
    "one_to_one": "||--||",
    "one_to_many": "||--o{",
    "many_to_many": "}o--o{",
    "foreign_key": "||--o{",
}


def load_tables(schema_dir: Path, pattern: Optional[str] = None) -> List[Dict]:
    """Load tables matching regex pattern."""
    tables = []
    regex = re.compile(pattern) if pattern else None

    for schema_path in schema_dir.iterdir():
        if not schema_path.is_dir() or schema_path.name.startswith("_"):
            continue

        for table_file in schema_path.glob("*.json"):
            table_name = table_file.stem
            if regex and not regex.search(table_name):
                continue

            with open(table_file, "r", encoding="utf-8") as f:
                tables.append(json.load(f))

    return tables


def load_relationships(schema_dir: Path) -> List[Dict]:
    """Load ER relationships from _relationships.json."""
    rel_file = schema_dir / "_relationships.json"
    if not rel_file.exists():
        return []

    with open(rel_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_rels = data.get("relationships", []) if isinstance(data, dict) else data

    normalized = []
    for rel in raw_rels:
        if isinstance(rel.get("source"), dict) and isinstance(rel.get("target"), dict):
            source = rel.get("source", {})
            target = rel.get("target", {})
            normalized.append({
                "from_schema": source.get("schema", "public"),
                "from_table": source.get("table", ""),
                "to_schema": target.get("schema", "public"),
                "to_table": target.get("table", ""),
                "type": rel.get("type", "foreign_key"),
            })
        else:
            normalized.append({
                "from_schema": rel.get("from_schema", "public"),
                "from_table": rel.get("from_table", ""),
                "to_schema": rel.get("to_schema", "public"),
                "to_table": rel.get("to_table", ""),
                "type": rel.get("type", "foreign_key"),
            })

    return normalized


def filter_relationships(
    relationships: List[Dict],
    table_names: Set[str],
) -> List[Dict]:
    """Filter and deduplicate relationships to only include selected tables."""
    filtered = []
    seen = set()  # Deduplicate by (from_table, to_table) pair

    for rel in relationships:
        from_table = rel.get("from_table", "").split(".")[-1]
        to_table = rel.get("to_table", "").split(".")[-1]

        if from_table in table_names and to_table in table_names:
            key = (from_table, to_table)
            if key in seen:
                continue
            seen.add(key)

            filtered.append({
                "from_schema": rel.get("from_schema", "public"),
                "from_table": from_table,
                "to_schema": rel.get("to_schema", "public"),
                "to_table": to_table,
                "type": rel.get("type", "foreign_key"),
            })

    return filtered


def compute_statistics(
    tables: List[Dict],
    relationships: List[Dict],
    schema_dir: Path
) -> Dict:
    """Compute statistics for diagram output."""
    tables_with_rels = set()
    for rel in relationships:
        tables_with_rels.add(rel["from_table"])
        tables_with_rels.add(rel["to_table"])

    selected_names = {t.get("name") for t in tables}
    isolated = sorted(selected_names - tables_with_rels)

    total = 0
    for schema_path in schema_dir.iterdir():
        if schema_path.is_dir() and not schema_path.name.startswith("_"):
            total += len(list(schema_path.glob("*.json")))

    return {
        "included_tables": len(tables),
        "total_tables": total,
        "relationships": len(relationships),
        "isolated_tables": isolated,
    }


def format_statistics(stats: Dict) -> str:
    """Format statistics for output."""
    lines = [
        "📊 Diagram Statistics:",
        f"  - Included tables: {stats['included_tables']}",
        f"  - Relationships: {stats['relationships']}",
        f"  - Total tables in schema: {stats['total_tables']}",
    ]

    if stats["isolated_tables"]:
        isolated_str = ", ".join(stats["isolated_tables"][:5])
        if len(stats["isolated_tables"]) > 5:
            isolated_str += f" ... ({len(stats['isolated_tables'])} total)"
        lines.append(f"  - Isolated tables: {isolated_str}")

    return "\n".join(lines)


# =============================================================================
# Mermaid Syntax Validation
# =============================================================================

def validate_mermaid_er(code: str) -> Tuple[bool, List[str]]:
    """
    Validate Mermaid ER diagram syntax.

    Returns: (is_valid, list_of_errors)
    """
    errors = []
    warnings = []

    # Extract just the mermaid code (remove markdown wrapper)
    mermaid_match = re.search(r'```mermaid\n(.*?)\n```', code, re.DOTALL)
    if mermaid_match:
        code = mermaid_match.group(1)

    # Remove init directives - handle multi-line directives with nested braces
    # Init directives start with %%{ and end with }%%
    code_lines = code.strip().split('\n')
    cleaned_lines = []
    in_init_directive = False
    for line in code_lines:
        if '%%{' in line:
            in_init_directive = True
        if not in_init_directive:
            cleaned_lines.append(line)
        if '}%%' in line:
            in_init_directive = False

    lines = cleaned_lines

    # Check for diagram type
    first_content_line = None
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('%%'):
            first_content_line = stripped
            break

    if not first_content_line:
        errors.append("Empty diagram - no content found")
        return False, errors

    if first_content_line != "erDiagram":
        # Not an ER diagram, skip ER-specific validation
        return True, []

    # ER Diagram specific validation
    current_entity = None
    in_entity_block = False
    entity_names = set()
    line_num = 0

    for i, line in enumerate(lines):
        line_num = i + 1
        stripped = line.strip()

        if not stripped or stripped.startswith('%%'):
            continue

        # Check for entity definition start
        entity_match = re.match(r'^(\w+)\s*\{', stripped)
        if entity_match:
            entity_name = entity_match.group(1)
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', entity_name):
                errors.append(f"Line {line_num}: Invalid entity name '{entity_name}'. Must start with letter or underscore.")
            entity_names.add(entity_name)
            current_entity = entity_name
            in_entity_block = True
            continue

        # Check for entity block end
        if stripped == '}':
            current_entity = None
            in_entity_block = False
            continue

        # Check for attribute definition inside entity
        if in_entity_block and current_entity:
            # Valid formats:
            #   TYPE attribute_name
            #   TYPE attribute_name PK
            #   TYPE attribute_name FK
            attr_pattern = r'^(\w+)\s+(\w+)(?:\s+(PK|FK))?$'
            attr_match = re.match(attr_pattern, stripped)

            if not attr_match:
                # Check for common mistakes
                if '"' in stripped:
                    errors.append(
                        f"Line {line_num}: ER diagrams should NOT use quotes around attribute names. "
                        f"Found: {stripped[:50]}"
                    )
                elif re.match(r'^(\w+)\s+"', stripped):
                    errors.append(
                        f"Line {line_num}: Remove quotes from attribute name. "
                        f"Use: TYPE attribute_name (not TYPE \"attribute_name\")"
                    )
                else:
                    errors.append(
                        f"Line {line_num}: Invalid attribute syntax. "
                        f"Expected: TYPE attribute_name [PK|FK], got: {stripped[:50]}"
                    )

        # Check for relationship definition
        rel_pattern = r'^(\w+)\s+(\|\|--\|\||\|\|--o\{|}o--o\{|\|\|--o\{)\s+(\w+)\s*:\s*".*"$'
        if not in_entity_block and re.match(rel_pattern, stripped):
            match = re.match(rel_pattern, stripped)
            from_entity = match.group(1)
            to_entity = match.group(3)

            if from_entity not in entity_names:
                warnings.append(f"Line {line_num}: Unknown entity '{from_entity}' in relationship")
            if to_entity not in entity_names:
                warnings.append(f"Line {line_num}: Unknown entity '{to_entity}' in relationship")

    # Check bracket balance - ONLY count entity blocks, not relationship notation
    # Relationship lines use ||--o{ or }o--o{ which contain braces but aren't grouping
    open_braces = 0
    close_braces = 0
    for line in lines:
        stripped = line.strip()
        # Skip relationship lines (contain relationship notation like ||--o{ or }o--o{)
        # These have the pattern: entity1 RELATIONSHIP entity2 : "label"
        if re.match(r'^\w+\s+(\||\{|o).*(:|")', stripped):
            continue
        open_braces += stripped.count('{')
        close_braces += stripped.count('}')

    if open_braces != close_braces:
        errors.append(f"Unbalanced braces: {open_braces} opening, {close_braces} closing")

    is_valid = len(errors) == 0

    if warnings:
        print("⚠️  Warnings:", file=sys.stderr)
        for w in warnings[:5]:
            print(f"  • {w}", file=sys.stderr)

    return is_valid, errors


# =============================================================================
# Diagram Generation
# =============================================================================

INIT_DIRECTIVE = """%%{init: {'theme': 'base', 'themeVariables': {
  'primaryColor': '#4f46e5', 'primaryTextColor': '#ffffff',
  'primaryBorderColor': '#3730a3', 'lineColor': '#94a3b8',
  'secondaryColor': '#10b981', 'tertiaryColor': '#f59e0b',
  'background': '#ffffff', 'mainBkg': '#f8fafc',
  'nodeBorder': '#cbd5e1', 'clusterBkg': '#f1f5f9',
  'clusterBorder': '#e2e8f0', 'titleColor': '#1e293b',
  'edgeLabelBackground': '#334155',
  'textColor': '#f1f5f9'
}}}%%"""


def normalize_mermaid_type(col_type: str) -> str:
    """Normalize SQL types to Mermaid-compatible types."""
    col_type_lower = col_type.lower()

    # Map common SQL types to Mermaid-friendly types
    type_mapping = {
        "text": "string",
        "varchar": "string",
        "char": "string",
        "nvarchar": "string",
        "nchar": "string",
        "bigint": "bigint",
        "integer": "int",
        "int": "int",
        "smallint": "int",
        "tinyint": "int",
        "decimal": "decimal",
        "numeric": "decimal",
        "float": "float",
        "double": "float",
        "real": "float",
        "boolean": "boolean",
        "bool": "boolean",
        "date": "date",
        "datetime": "datetime",
        "timestamp": "datetime",
        "time": "string",
        "blob": "blob",
        "bytea": "blob",
        "json": "json",
        "jsonb": "json",
        "uuid": "string",
        "array": "string",
    }

    # Check for exact match first
    if col_type_lower in type_mapping:
        return type_mapping[col_type_lower]

    # Check for partial match (e.g., "varchar(255)" -> "string")
    for sql_type, mermaid_type in type_mapping.items():
        if col_type_lower.startswith(sql_type):
            return mermaid_type

    # Default fallback - use string for unknown types
    return "string"


def sanitize_column_name(col_name: str) -> str:
    """Sanitize column name for Mermaid compatibility."""
    # Replace spaces and special characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    return sanitized if sanitized else "column"


def generate_er(tables: list, relationships: list, max_columns: Optional[int] = None) -> str:
    """Generate ER diagram with proper relationship notation."""
    lines = [INIT_DIRECTIVE, "erDiagram"]

    # Add table definitions
    for table in tables:
        name = table.get("name", "unknown")
        # Sanitize table name
        safe_name = sanitize_column_name(name)
        lines.append(f"    {safe_name} {{")

        # Get columns and prioritize PK/FK
        columns = table.get("columns", {})
        col_items = list(columns.items())

        pk_cols = [(k, v) for k, v in col_items if v.get("pk") or v.get("primary_key")]
        fk_cols = [(k, v) for k, v in col_items if v.get("fk") and not (v.get("pk") or v.get("primary_key"))]
        other_cols = [(k, v) for k, v in col_items if not (v.get("pk") or v.get("primary_key")) and not v.get("fk")]

        prioritized = pk_cols + fk_cols + other_cols

        # Apply column limit
        if max_columns and len(prioritized) > max_columns:
            prioritized = prioritized[:max_columns]

        for col_name, col_info in prioritized:
            col_type = normalize_mermaid_type(col_info.get("type", "string"))
            is_pk = col_info.get("pk", False) or col_info.get("primary_key", False)
            is_fk = col_info.get("fk") is not None

            # Sanitize column name (NO quotes in Mermaid ER)
            safe_col_name = sanitize_column_name(col_name)

            if is_pk:
                lines.append(f'        {col_type} {safe_col_name} PK')
            elif is_fk:
                lines.append(f'        {col_type} {safe_col_name} FK')
            else:
                lines.append(f'        {col_type} {safe_col_name}')

        lines.append("    }")

    # Build a mapping from original table names to sanitized names
    name_mapping = {}
    for table in tables:
        original = table.get("name", "unknown")
        sanitized = sanitize_column_name(original)
        name_mapping[original] = sanitized

    # Add relationships with proper notation
    for rel in relationships:
        from_t = name_mapping.get(rel["from_table"], sanitize_column_name(rel["from_table"]))
        to_t = name_mapping.get(rel["to_table"], sanitize_column_name(rel["to_table"]))
        rel_type = rel.get("type", "foreign_key")

        notation = RELATIONSHIP_STYLES.get(rel_type, RELATIONSHIP_STYLES["foreign_key"])
        lines.append(f'    {from_t} {notation} {to_t} : ""')

    return "\n".join(lines)


def generate_lineage(tables: list, relationships: list, direction: str) -> str:
    """Generate lineage flowchart."""
    lines = [INIT_DIRECTIVE, f"flowchart {direction}"]

    # Add table nodes
    for table in tables:
        name = table.get("name", "unknown")
        lines.append(f'    {name}["{name}"]')

    # Add edges
    for rel in relationships:
        rel_type = rel.get("type", "FK")
        lines.append(f'    {rel["from_table"]} -->|"{rel_type}"| {rel["to_table"]}')

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Mermaid diagrams from schema"
    )
    parser.add_argument("--schema-dir", required=True, help="Schema directory")
    parser.add_argument("--type", default="relationship", choices=["relationship", "lineage"],
                        help="Diagram type (default: relationship)")
    parser.add_argument("--direction", default="LR", choices=["TB", "LR", "RL", "BT"])
    parser.add_argument("--pattern", help="Regex pattern for table names")
    parser.add_argument("--max-columns", type=int, default=15,
                        help="Maximum columns per table (prioritizes PK/FK)")
    parser.add_argument("--output", required=True, help="Output markdown file")

    args = parser.parse_args()
    schema_dir = Path(args.schema_dir)

    if not schema_dir.exists():
        print(f"Error: Schema directory not found: {schema_dir}", file=sys.stderr)
        sys.exit(1)

    # Load tables and relationships
    tables = load_tables(schema_dir, args.pattern)

    if not tables:
        print("No tables found matching criteria.", file=sys.stderr)
        sys.exit(1)

    table_names = {t.get("name") for t in tables}
    relationships = load_relationships(schema_dir)
    relationships = filter_relationships(relationships, table_names)

    # Generate diagram
    if args.type == "relationship":
        code = generate_er(tables, relationships, args.max_columns)
    else:
        code = generate_lineage(tables, relationships, args.direction)

    # Wrap in Markdown format
    output_content = f"""# Database Diagram

```mermaid
{code}
```

<!-- Generated by mermaid-studio -->
"""

    # Validate the generated Mermaid code
    is_valid, errors = validate_mermaid_er(output_content)
    if not is_valid:
        print("❌ Mermaid validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  • {error}", file=sys.stderr)
        sys.exit(1)

    # Write output
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(output_content, encoding="utf-8")

    # Output statistics
    stats = compute_statistics(tables, relationships, schema_dir)

    print(format_statistics(stats))
    print(f"Generated {args.type} diagram: {output}")


if __name__ == "__main__":
    main()
