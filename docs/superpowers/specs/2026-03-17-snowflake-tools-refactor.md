# Snowflake Tools 重构设计

**状态**: ✅ 已完成
**完成日期**: 2026-03-17

## 背景

当前实现存在以下问题：
1. **每次查询创建新连接** — 没有连接池，性能差
2. **没有 SQL 验证** — 可能执行错误 SQL
3. **工具分散** — `snowflake_tool.py` 和 `db_explorer_tool.py` 分离
4. **不符合 LangChain 最佳实践** — 自己造轮子

## 方案：使用 LangChain SQLDatabaseToolkit

LangChain 提供标准的 `SQLDatabaseToolkit`，包含 4 个工具：

| 工具 | 功能 | 对应当前实现 |
|------|------|-------------|
| `sql_db_query` | 执行 SQL 查询 | `snowflake_query` |
| `sql_db_schema` | 获取表结构 + 示例数据 | `describe_table` + `preview_table` |
| `sql_db_list_tables` | 列出所有表 | `list_tables` |
| `sql_db_query_checker` | 验证 SQL 正确性 | **新增** |

## 架构设计

```
src/
├── core/
│   ├── config.py          # 配置管理（不变）
│   └── database.py        # 数据库连接（重构：添加连接池）
├── agent/
│   └── tools/
│       ├── __init__.py    # 导出 get_snowflake_tools()
│       └── snowflake.py   # SQLDatabaseToolkit 封装
└── ...
```

## 实现细节

### 1. 添加依赖

```txt
# requirements.txt 新增
langchain-community>=0.3.0
snowflake-sqlalchemy>=1.6.0
```

### 2. Snowflake SQLAlchemy URL 格式

```python
# 格式
snowflake://<user>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>&role=<role>

# 示例
snowflake://myuser:mypass@xy12345.us-east-1/ENT_HACKATHON_DATA_SHARE/EA_HACKATHON?warehouse=COMPUTE_WH
```

### 3. 连接池配置

SQLAlchemy 内置连接池，默认配置：
- `pool_size=5` — 连接池大小
- `max_overflow=10` — 最大溢出连接
- `pool_pre_ping=True` — 连接健康检查
- `pool_recycle=3600` — 连接回收时间（秒）

### 4. Tools 实现

```python
# src/agent/tools/snowflake.py
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.tools import BaseTool

from src.core.config import get_settings


def get_snowflake_db() -> SQLDatabase:
    """创建 Snowflake 数据库连接（带连接池）。"""
    settings = get_settings()

    # 构建 Snowflake SQLAlchemy URL
    # 格式: snowflake://user:password@account/database/schema?warehouse=WH
    account = settings.snowflake_account
    user = settings.snowflake_user
    password = settings.snowflake_password
    database = settings.snowflake_database
    schema = settings.snowflake_schema
    warehouse = settings.snowflake_warehouse
    role = settings.snowflake_role

    # URL encode password (处理特殊字符)
    from urllib.parse import quote_plus
    password_encoded = quote_plus(password)

    url = f"snowflake://{user}:{password_encoded}@{account}/{database}/{schema}?warehouse={warehouse}"
    if role:
        url += f"&role={role}"

    # 创建连接（SQLAlchemy 自动管理连接池）
    db = SQLDatabase.from_uri(
        url,
        engine_args={
            "pool_size": 5,
            "max_overflow": 10,
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        }
    )
    return db


def get_snowflake_tools(llm) -> list[BaseTool]:
    """获取 Snowflake 工具集。

    返回 4 个标准工具：
    - sql_db_query: 执行 SQL 查询
    - sql_db_schema: 获取表结构
    - sql_db_list_tables: 列出表
    - sql_db_query_checker: 验证 SQL
    """
    db = get_snowflake_db()
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    return toolkit.get_tools()
```

### 5. Agent 集成

```python
# src/agent/agent.py (修改)
from langchain.chat_models import init_chat_model
from src.agent.tools import get_snowflake_tools

def create_az_data_agent(settings=None):
    settings = settings or get_settings()

    # 初始化 LLM
    model = init_chat_model(settings.llm_model)

    # 获取 Snowflake 工具（使用标准 Toolkit）
    tools = get_snowflake_tools(model)

    # ... 其他配置
```

## 迁移计划

### Phase 1: 添加新实现
1. 添加依赖到 requirements.txt
2. 创建 `src/agent/tools/snowflake.py`
3. 更新 `src/agent/tools/__init__.py`

### Phase 2: 清理旧代码
1. 删除 `src/agent/tools/snowflake_tool.py`
2. 删除 `src/agent/tools/db_explorer_tool.py`
3. 更新 `src/core/database.py`（保留用于非 Agent 场景）

### Phase 3: 测试验证
1. 单元测试
2. 集成测试
3. 端到端测试

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| snowflake-sqlalchemy 版本兼容 | 使用稳定版本 >=1.6.0 |
| 连接池配置不当 | 使用保守默认值，支持配置覆盖 |
| 密码特殊字符 | URL encode 处理 |

## 后续优化

1. **缓存表结构** — 减少重复查询
2. **查询超时配置** — 防止长时间运行
3. **只读模式** — 生产环境安全