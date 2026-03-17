---
name: db-toolkit
description: |
  数据库操作工具集。提供连接管理、SQL执行、Schema探索、数据导出能力。

  ## 功能
  - 连接多种数据库 (SQLite, PostgreSQL, MySQL, MariaDB)
  - 执行 SQL 查询 (CSV 输出，分页导出)
  - 探索数据库 Schema (表、列、索引)
  - 管理数据库连接配置

  ## 配置
  在 `.amandax/settings.json` 中配置数据库连接：
  ```json
  {
    "database": {
      "bird_testing": {
        "type": "postgresql",
        "connectionString": "postgresql://user:password@host:5432/database"
      }
    }
  }
  ```

  ## Connection String 格式
  | 数据库 | 格式 |
  |--------|------|
  | PostgreSQL | `postgresql://user:password@host:5432/database` |
  | MySQL | `mysql://user:password@host:3306/database` |
  | SQLite | `sqlite:///path/to/database.db` |

  ## 相关 Skills
  此工具集可配合以下分析 skills 使用：
  - sql-optimization: SQL 性能优化
  - sql-optimization-patterns: SQL 优化模式
  - sql-code-review: SQL 代码审查
  - data-quality-frameworks: 数据质量验证
  - data-analyst: 数据分析
  - kpi-dashboard-design: KPI 计算

allowed-tools: bash execute read_file write_file
---

# DB Toolkit

数据库基础设施工具集，支持 SQLite、PostgreSQL、MySQL/MariaDB。

**脚本位置**: `.amandax/skills/db-toolkit/scripts/`

## Scripts Reference

### `connect.py` - 连接管理

管理 `.amandax/settings.json` 中的数据库连接配置。

```bash
# 列出所有连接
python .amandax/skills/db-toolkit/scripts/connect.py --list

# 添加 PostgreSQL 连接
python .amandax/skills/db-toolkit/scripts/connect.py --add postgresql --name bird_testing \
    --connection-string "postgresql://user:password@host:5432/database"

# 添加 MySQL 连接
python .amandax/skills/db-toolkit/scripts/connect.py --add mysql --name local_mysql \
    --connection-string "mysql://root:password@localhost:3306/mydb"

# 添加 SQLite 连接
python .amandax/skills/db-toolkit/scripts/connect.py --add sqlite --name local_sqlite \
    --connection-string "sqlite:///./data.db"

# 测试连接
python .amandax/skills/db-toolkit/scripts/connect.py --test bird_testing

# 删除连接
python .amandax/skills/db-toolkit/scripts/connect.py --remove bird_testing
```

### `search.py` - Schema 探索

渐进式披露模式探索数据库结构。

```bash
# 探索表 (names: 仅名称)
python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type table --detail names

# 探索表 (summary: 名称 + 元数据)
python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type table --pattern "%user%" --detail summary

# 探索列 (full: 完整结构)
python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type column --table users --detail full

# 探索索引
python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type index --table users --detail full
```

**Detail Levels:**
| Level | Description | Use Case |
|-------|-------------|----------|
| `names` | 仅对象名称 | 浏览、快速查找 |
| `summary` | 名称 + 元数据 | 选择相似表 |
| `full` | 完整结构 | 编写查询前 |

### `execute.py` - SQL 执行

执行 SQL 查询并输出 CSV 格式结果。

**默认行为**: 输出到 stdout (CSV 格式，最多 100 行，超出提示总行数)
**--output**: 保存完整结果到文件 (无行数限制)

```bash
# 执行查询并输出到 stdout (默认，最多 100 行)
python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM users LIMIT 10"

# 保存完整结果到 artifacts 目录
python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM users" --output artifacts/query_result/users.csv

# 分页导出大表
python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM trans" --paginate 10000 --output artifacts/batches/trans_

# 限制返回行数
python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM users" --limit 100 --offset 50
```

### `schema_profiler.py` - 导出 Schema 到 JSON

导出数据库 Schema 到分布式 JSON 文件结构。

```bash
# 导出整个数据库
python .amandax/skills/db-toolkit/scripts/schema_profiler.py --database bird_testing --output-dir artifacts/schemas

# 导出指定 schema
python .amandax/skills/db-toolkit/scripts/schema_profiler.py --database bird_testing --schema public --output-dir artifacts/schemas

# 导出单个表
python .amandax/skills/db-toolkit/scripts/schema_profiler.py --database bird_testing --table users --output-dir artifacts/schemas
```

Output structure:
```
artifacts/schemas/bird_testing/
├── _metadata.json           # Database metadata
├── _relationships.json      # ER relationships (foreign keys)
├── _lineages.json           # Data lineage (transformation flows)
└── public/
    ├── users.json
    └── orders.json
```

**File format (dbt-style):**
- `_relationships.json`: ER relationships with `unique_id`, `source`, `target`, `type`, `metadata` fields
- `_lineages.json`: Data lineage for transformation flows (created empty, populated manually or via ETL tools)

### `schema_converter.py` - 格式转换

DDL SQL 与分布式 JSON 之间转换。

```bash
# DDL 转分布式 JSON
python .amandax/skills/db-toolkit/scripts/schema_converter.py --input artifacts/ddl/schema.sql --from ddl --to distributed \
    --database mydb --output-dir artifacts/schemas

# 分布式 JSON 转单文件 JSON
python .amandax/skills/db-toolkit/scripts/schema_converter.py --input-dir artifacts/schemas/mydb --from distributed \
    --to json --output artifacts/schema.json
```

### `schema_validator.py` - Schema 验证

验证 Schema JSON 文件的格式合规性和设计质量。

```bash
# 验证 Schema
python .amandax/skills/db-toolkit/scripts/schema_validator.py --schema-dir artifacts/schemas/mydb

# 详细输出
python .amandax/skills/db-toolkit/scripts/schema_validator.py --schema-dir artifacts/schemas/mydb --verbose

# 输出报告到文件
python .amandax/skills/db-toolkit/scripts/schema_validator.py --schema-dir artifacts/schemas/mydb --output report.json
```

**格式检查** (errors):
- `_relationships.json`: version, database, relationships, unique_id, source/target
- `_lineages.json`: version, database, lineages
- 表文件: schema, name, columns, column.type

**质量检查** (warnings):
- 无主键的表
- 无注释的列
- 命名规范 (snake_case)
- 可疑类型 (如 price 用 VARCHAR)

## Dependencies

```bash
pip install python-dotenv
pip install psycopg2-binary      # PostgreSQL
pip install mysql-connector-python  # MySQL/MariaDB
```
