# AZ Data Agent 技术文档

**版本**: 1.0.0
**更新日期**: 2026-03-16
**项目类型**: Hackathon 数据分析平台

---

## 目录

1. [项目概述](#1-项目概述)
2. [系统架构](#2-系统架构)
3. [模块详解](#3-模块详解)
4. [API 参考](#4-api-参考)
5. [数据模型](#5-数据模型)
6. [配置管理](#6-配置管理)
7. [部署指南](#7-部署指南)
8. [开发规范](#8-开发规范)
9. [测试指南](#9-测试指南)

---

## 1. 项目概述

### 1.1 项目背景

AZ Data Agent 是一个基于 AstraZeneca 医药数据构建的 AI Agent 数据分析系统，旨在为业务分析师提供智能化的数据查询、监控和分析能力。

### 1.2 核心功能

| 功能模块 | 描述 |
|---------|------|
| **智能监控** | 定时执行 SQL 监控指标，自动检测异常并触发告警 |
| **AI 分析** | LangChain Agent 自动分析数据，回答用户自然语言问题 |
| **自动告警** | 异常触发时自动调用 Agent 进行深度分析 |
| **Web UI** | Streamlit 实现 Web 交互界面，支持对话式查询 |

### 1.3 技术栈概览

| 层级 | 技术选型 | 版本要求 |
|------|---------|----------|
| 编程语言 | Python | 3.10+ |
| LLM 框架 | LangChain | 0.3.0+ |
| LLM 模型 | Claude / OpenAI / Azure OpenAI | 可配置 |
| 数据仓库 | Snowflake | 3.0.0+ |
| Web 框架 | Streamlit | 1.30.0+ |
| 任务调度 | APScheduler | 3.10.0+ |
| 配置管理 | Pydantic Settings | 2.0.0+ |
| 数据可视化 | Plotly | 5.0.0+ |
| 数据处理 | Pandas | 2.0.0+ |

### 1.4 项目结构

```
az-data-agent/
├── src/                          # 源代码
│   ├── core/                     # 核心共享模块
│   │   ├── config.py            # 配置管理
│   │   ├── database.py          # Snowflake 数据库连接
│   │   └── llm_provider.py      # LLM 提供商抽象层
│   │
│   ├── monitor/                  # 监控服务模块
│   │   ├── models.py            # SQLAlchemy 数据模型
│   │   ├── metrics_loader.py    # 指标配置加载器
│   │   ├── executor.py          # SQL 执行器
│   │   ├── alert_engine.py      # 告警判断与触发
│   │   └── scheduler.py         # APScheduler 定时任务
│   │
│   ├── agent/                    # Data Agent 模块
│   │   ├── agent.py             # LangChain Agent 主程序
│   │   ├── middleware/          # 中间件
│   │   │   └── context_enricher.py
│   │   ├── skills/              # Skills 能力模块
│   │   │   ├── base.py
│   │   │   └── sql_analyzer.py
│   │   └── tools/               # Agent Tools
│   │       ├── snowflake_tool.py
│   │       └── chart_tool.py
│   │
│   ├── web/                      # Web UI 模块
│   │   ├── app.py               # Streamlit 主入口
│   │   └── components/          # UI 组件
│   │       └── chat.py
│   │
│   └── messaging/                # 消息队列模块
│       └── queue.py             # SQLite 队列实现
│
├── config/                       # 配置文件
│   ├── settings.yaml            # 主配置文件
│   ├── metrics_template.yaml    # 预定义指标模板
│   └── prompts/                 # Agent 提示词
│       └── system_prompt.md
│
├── tests/                        # 测试目录
│   ├── test_core/               # 核心模块测试
│   ├── test_monitor/            # 监控模块测试
│   └── test_agent/              # Agent 模块测试
│
├── docs/                         # 文档目录
├── pyproject.toml               # 项目配置
└── README.md                    # 项目说明
```

---

## 2. 系统架构

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AZ Data Agent System                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐            │
│  │   Web UI     │     │   Monitor    │     │  Data Agent  │            │
│  │  (Streamlit) │     │   Service    │     │   Service    │            │
│  │  Port: 8501  │     │  Background  │     │  On-Demand   │            │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘            │
│         │                    │                    │                     │
│         │                    ▼                    │                     │
│         │            ┌──────────────┐            │                     │
│         │            │ Alert Engine │            │                     │
│         │            └──────┬───────┘            │                     │
│         │                   │                    │                     │
│         └───────────────────┴────────────────────┘                     │
│                             │                                           │
│                  ┌──────────┴──────────┐                               │
│                  │   Shared Services   │                               │
│                  │  ┌───────────────┐  │                               │
│                  │  │ Config Mgr    │  │                               │
│                  │  │ LLM Provider  │  │                               │
│                  │  │ Snowflake DB  │  │                               │
│                  │  │ SQLite Queue  │  │                               │
│                  │  └───────────────┘  │                               │
│                  └─────────────────────┘                               │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    ┌───────────────────────────────┐
                    │      Snowflake Data Warehouse │
                    │   ENT_HACKATHON_DATA_SHARE.   │
                    │         EA_HACKATHON          │
                    └───────────────────────────────┘
```

### 2.2 核心数据流

#### 2.2.1 用户交互流程

```
用户输入问题
     │
     ▼
┌─────────────┐
│  Web UI     │
│  Streamlit  │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌──────────────┐
│ Data Agent  │────▶│ LLM Provider │
│ LangChain   │     │ Claude/OpenAI│
└──────┬──────┘     └──────────────┘
       │
       ▼
┌─────────────┐
│ Snowflake   │
│ Tool        │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Snowflake   │
│ Data        │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Chart Tool  │
│ Plotly      │
└──────┬──────┘
       │
       ▼
  结果展示
```

#### 2.2.2 监控告警流程

```
┌─────────────────┐
│   Scheduler     │
│   APScheduler   │
│   (每5分钟)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Executor      │
│   执行 SQL       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Alert Engine   │
│  阈值判断        │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
  正常       告警
             │
             ▼
    ┌─────────────────┐
    │  Alert Queue    │
    │  SQLite         │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  Data Agent     │
    │  自动分析        │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────┐
    │  分析结果存储    │
    └─────────────────┘
```

### 2.3 Agent 架构

```
┌──────────────────────────────────────────────────────────────────┐
│                    Data Analysis Agent                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    Middleware Stack                          │ │
│  │  ┌───────────────────┐  ┌────────────────────────────────┐  │ │
│  │  │ContextEnricher    │  │ 注入业务上下文、数据模型信息    │  │ │
│  │  │Middleware         │  │                                │  │ │
│  │  └───────────────────┘  └────────────────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                              │                                    │
│                              ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                      Tools Layer                             │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │ │
│  │  │SnowflakeTool│  │ ChartTool   │  │  (可扩展更多工具)    │  │ │
│  │  │ SQL 执行    │  │ 图表生成    │  │                     │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. 模块详解

### 3.1 Core 核心模块

核心模块提供应用的基础设施，包括配置管理、数据库连接、LLM 提供商抽象。

#### 3.1.1 配置管理 (config.py)

**职责**: 使用 Pydantic Settings 管理应用配置，支持环境变量和 .env 文件。

**核心类**: `Settings`

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|-------|------|
| `llm_provider` | Literal | "claude" | LLM 提供商 (claude/openai/azure) |
| `llm_model` | str | "claude-sonnet-4-5-20250929" | 模型名称 |
| `llm_api_key` | str | "" | API 密钥 |
| `snowflake_account` | str | "" | Snowflake 账号 |
| `snowflake_user` | str | "" | Snowflake 用户名 |
| `snowflake_password` | str | "" | Snowflake 密码 |
| `snowflake_warehouse` | str | "COMPUTE_WH" | 计算仓库 |
| `snowflake_database` | str | "ENT_HACKATHON_DATA_SHARE" | 数据库 |
| `snowflake_schema` | str | "EA_HACKATHON" | Schema |
| `monitor_interval_minutes` | int | 5 | 监控间隔（分钟） |

**使用示例**:

```python
from src.core.config import get_settings

settings = get_settings()
print(settings.llm_provider)  # "claude"
```

#### 3.1.2 数据库连接 (database.py)

**职责**: 管理 Snowflake 数据库连接，提供查询执行接口。

**核心函数**:

| 函数 | 参数 | 返回值 | 说明 |
|------|------|-------|------|
| `get_snowflake_connection` | `settings: Settings` | `SnowflakeConnection` | 创建数据库连接 |
| `execute_query` | `sql, settings, params=None` | `list[tuple]` | 执行查询返回结果 |
| `execute_query_with_columns` | `sql, settings, params=None` | `tuple[list[str], list[tuple]]` | 执行查询返回列名和结果 |

**使用示例**:

```python
from src.core.database import execute_query_with_columns
from src.core.config import get_settings

settings = get_settings()
columns, rows = execute_query_with_columns(
    "SELECT * FROM FACT_PNL_BASE_BRAND LIMIT 10",
    settings
)
```

#### 3.1.3 LLM 提供商 (llm_provider.py)

**职责**: 抽象 LLM 后端，支持 Claude、OpenAI、Azure OpenAI。

**核心函数**: `get_llm(settings, provider=None)`

| 提供商 | 返回类型 | 配置要求 |
|-------|---------|---------|
| claude | `ChatAnthropic` | `llm_api_key` |
| openai | `ChatOpenAI` | `llm_api_key` |
| azure | `AzureChatOpenAI` | `llm_api_key`, `azure_openai_endpoint`, `azure_openai_deployment` |

**使用示例**:

```python
from src.core.llm_provider import get_llm
from src.core.config import get_settings

settings = get_settings()
llm = get_llm(settings)
response = llm.invoke("Hello!")
```

---

### 3.2 Monitor 监控模块

监控模块负责定时执行业务指标监控，检测异常并触发告警。

#### 3.2.1 数据模型 (models.py)

**核心实体**:

```
┌─────────────┐       ┌─────────────────┐       ┌─────────────┐
│   Metric    │       │  MetricResult   │       │ AlertQueue  │
├─────────────┤       ├─────────────────┤       ├─────────────┤
│ id          │───┐   │ id              │   ┌───│ id          │
│ name        │   │   │ metric_id (FK)  │───┘   │ metric_id   │
│ description │   └──▶│ executed_at     │       │ result_id   │
│ category    │       │ actual_value    │       │ status      │
│ sql_template│       │ threshold_value │       │ analysis_   │
│ threshold_  │       │ is_alert        │       │   result    │
│   type      │       └─────────────────┘       │ created_at  │
│ threshold_  │                                 │ processed_at│
│   value     │                                 └─────────────┘
│ threshold_  │
│   operator  │
│ is_active   │
│ created_at  │
└─────────────┘
```

**枚举类型**:

| 枚举 | 值 |
|------|-----|
| `ThresholdType` | ABSOLUTE, PERCENTAGE, CHANGE |
| `ThresholdOperator` | GT, LT, EQ, GTE, LTE |
| `AlertStatus` | PENDING, PROCESSING, COMPLETED, FAILED |

#### 3.2.2 调度器 (scheduler.py)

**核心类**: `MonitorScheduler`

| 方法 | 说明 |
|------|------|
| `start()` | 启动调度器 |
| `stop()` | 停止调度器 |
| `run_once()` | 执行一次所有活跃指标 |
| `run_forever()` | 阻塞运行调度器 |

**使用示例**:

```python
from src.monitor.scheduler import MonitorScheduler

scheduler = MonitorScheduler(interval_seconds=300)
scheduler.start()
# 后台运行，每 5 分钟执行一次
```

#### 3.2.3 告警引擎 (alert_engine.py)

**核心函数**:

| 函数 | 说明 |
|------|------|
| `check_threshold(actual, threshold, operator)` | 检查是否触发告警 |
| `process_metric(metric, settings, db_path)` | 执行指标并创建结果记录 |
| `get_pending_alerts(db_path)` | 获取待处理告警 |
| `complete_alert(alert_id, analysis_result, db_path)` | 完成告警处理 |

---

### 3.3 Agent 模块

Agent 模块是核心的 AI 分析引擎，基于 LangChain 构建。

#### 3.3.1 Agent 主程序 (agent.py)

**核心函数**:

| 函数 | 说明 |
|------|------|
| `create_data_agent(settings, verbose)` | 创建数据分析 Agent |
| `analyze_with_agent(question, settings)` | 使用 Agent 分析问题 |

**Agent 配置**:

```python
# 工具列表
tools = [
    SnowflakeTool(),  # SQL 查询工具
    ChartTool(),      # 图表生成工具
]

# 中间件
middleware = ContextEnricherMiddleware()

# 系统提示词
SYSTEM_PROMPT = """
You are an AI data analyst for AstraZeneca pharmaceutical data.
...
"""
```

#### 3.3.2 工具 (Tools)

##### SnowflakeTool

| 属性 | 值 |
|------|-----|
| `name` | "snowflake_query" |
| `description` | 执行 Snowflake SQL 查询 |

**输入参数**:

```python
class SnowflakeToolInput(BaseModel):
    sql: str  # SQL 查询语句
```

**可用表**:
- `FACT_PNL_BASE_BRAND`: P&L 财务指标
- `FACT_COM_BASE_BRAND`: 商业/市场指标
- `DIM_ACCOUNT`: P&L 科目
- `DIM_PRODUCT`: 产品/品牌
- `DIM_MARKET`: 治疗市场
- `DIM_TIME`: 时间维度
- `DIM_SCENARIO`: 计划场景

##### ChartTool

| 属性 | 值 |
|------|-----|
| `name` | "create_chart" |
| `description` | 创建数据可视化图表 |

**输入参数**:

```python
class ChartToolInput(BaseModel):
    data: str       # JSON 格式数据
    chart_type: str # 图表类型: bar, line, pie, scatter
    title: str      # 图表标题
```

#### 3.3.3 中间件 (Middleware)

##### ContextEnricherMiddleware

**职责**: 注入业务上下文到 Agent 提示词。

**注入内容**:
- 业务领域信息
- 数据模型描述
- 关键指标说明
- 地理范围
- 产品类别

---

### 3.4 Web 模块

Web 模块提供 Streamlit 用户界面。

#### 3.4.1 主应用 (app.py)

**功能**:
- 对话式交互界面
- 会话状态管理
- 错误处理和展示

**Session State**:

| 状态键 | 类型 | 说明 |
|-------|------|------|
| `messages` | `list[dict]` | 对话历史 |
| `agent` | `AgentExecutor` | Agent 实例 |

**示例问题**:
- "What is our current quarter revenue?"
- "Show budget variance by product"
- "What is AZ's market share in Oncology?"
- "Compare revenue YoY by therapeutic area"

---

## 4. API 参考

### 4.1 Core 模块 API

#### src.core.config

```python
class Settings(BaseSettings):
    """应用配置类"""

    # 类属性
    llm_provider: Literal["claude", "openai", "azure"]
    llm_model: str
    llm_api_key: str
    # ... 其他配置项

def get_settings() -> Settings:
    """获取全局配置实例"""

def reset_settings() -> None:
    """重置配置实例（用于测试）"""
```

#### src.core.database

```python
def get_snowflake_connection(settings: Settings) -> SnowflakeConnection:
    """创建 Snowflake 连接"""

def execute_query(
    sql: str,
    settings: Settings,
    params: dict[str, Any] | None = None
) -> list[tuple]:
    """执行 SQL 查询"""

def execute_query_with_columns(
    sql: str,
    settings: Settings,
    params: dict[str, Any] | None = None
) -> tuple[list[str], list[tuple]]:
    """执行 SQL 并返回列名"""
```

#### src.core.llm_provider

```python
def get_llm(
    settings: Settings,
    provider: Literal["claude", "openai", "azure"] | None = None
) -> ChatAnthropic | ChatOpenAI | AzureChatOpenAI:
    """获取 LLM 实例"""

class UnsupportedProviderError(Exception):
    """不支持的提供商异常"""
```

### 4.2 Monitor 模块 API

#### src.monitor.scheduler

```python
class MonitorScheduler:
    """监控调度器"""

    def __init__(
        self,
        interval_seconds: int | None = None,
        db_path: str = "data/monitor.db",
        settings: Settings | None = None
    ): ...

    @property
    def is_running(self) -> bool: ...

    def get_active_metrics(self) -> list[Metric]: ...
    def run_once(self) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def run_forever(self) -> None: ...

def start_monitor_service(
    db_path: str = "data/monitor.db",
    settings: Settings | None = None
) -> MonitorScheduler:
    """启动监控服务"""
```

#### src.monitor.alert_engine

```python
def check_threshold(
    actual_value: float,
    threshold_value: float,
    operator: ThresholdOperator
) -> bool:
    """检查阈值"""

def process_metric(
    metric: Metric,
    settings: Settings,
    db_path: str = "data/monitor.db"
) -> MetricResult:
    """处理指标"""

def get_pending_alerts(db_path: str) -> list[AlertQueue]:
    """获取待处理告警"""

def complete_alert(
    alert_id: int,
    analysis_result: str,
    db_path: str = "data/monitor.db"
) -> None:
    """完成告警"""
```

### 4.3 Agent 模块 API

#### src.agent.agent

```python
def create_data_agent(
    settings: Settings | None = None,
    verbose: bool = False
) -> AgentExecutor:
    """创建数据分析 Agent"""

def analyze_with_agent(
    question: str,
    settings: Settings | None = None
) -> str:
    """使用 Agent 分析问题"""
```

#### src.agent.tools.snowflake_tool

```python
class SnowflakeToolInput(BaseModel):
    sql: str

class SnowflakeTool(BaseTool):
    name: str = "snowflake_query"

    def _run(self, sql: str) -> str: ...
    async def _arun(self, sql: str) -> str: ...
```

#### src.agent.tools.chart_tool

```python
class ChartToolInput(BaseModel):
    data: str
    chart_type: str
    title: str = ""

class ChartTool(BaseTool):
    name: str = "create_chart"

    def _run(self, data: str, chart_type: str, title: str = "") -> str: ...
    async def _arun(self, data: str, chart_type: str, title: str = "") -> str: ...
```

---

## 5. 数据模型

### 5.1 Snowflake 数据仓库

**数据库路径**: `ENT_HACKATHON_DATA_SHARE.EA_HACKATHON`

#### 5.1.1 事实表

| 表名 | 说明 | 主要字段 |
|------|------|---------|
| `FACT_PNL_BASE_BRAND` | P&L 财务数据 | BUD_VALUE, BUD_VARIANCE, PY_VALUE, PY_VARIANCE |
| `FACT_COM_BASE_BRAND` | 商业/市场数据 | VALUE, MARKET_SHARE |

#### 5.1.2 维度表

| 表名 | 说明 | 主要字段 |
|------|------|---------|
| `DIM_ACCOUNT` | P&L 科目 | ACCOUNT_KEY, ACCOUNT_NAME, ACCOUNT_TYPE |
| `DIM_PRODUCT` | 产品/品牌 | PRODUCT_KEY, PRODUCT_NAME, AZ_PROD_IND |
| `DIM_MARKET` | 治疗市场 | MARKET_KEY, MARKET_NAME, TA_SEGMENT |
| `DIM_TIME` | 时间维度 | TIME_KEY, YEAR, QUARTER, IS_CURRENT_QUARTER |
| `DIM_SCENARIO` | 计划场景 | SCENARIO_KEY, SCENARIO_NAME |

#### 5.1.3 数据特点

- **星型模型**: 双事实表设计
- **时间范围**: 36 个月历史数据 (2023-2025)
- **多场景**: Actual, Budget, MTP, LTP
- **地理范围**: Spain (44000ES), Brazil (44000BR)
- **治疗领域**: Oncology, BioPharma (CVRM), Rare Disease, Central

### 5.2 SQLite 监控数据库

**位置**: `data/monitor.db`

#### 5.2.1 表结构

```sql
-- 指标定义表
CREATE TABLE metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,
    sql_template TEXT NOT NULL,
    threshold_type TEXT NOT NULL,  -- absolute | percentage | change
    threshold_value REAL NOT NULL,
    threshold_operator TEXT NOT NULL,  -- gt | lt | eq | gte | lte
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 指标执行结果表
CREATE TABLE metric_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_id INTEGER NOT NULL,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actual_value REAL,
    threshold_value REAL,
    is_alert BOOLEAN,
    FOREIGN KEY (metric_id) REFERENCES metrics(id)
);

-- 告警队列表
CREATE TABLE alert_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_id INTEGER NOT NULL,
    result_id INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',  -- pending | processing | completed | failed
    analysis_result TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    FOREIGN KEY (metric_id) REFERENCES metrics(id),
    FOREIGN KEY (result_id) REFERENCES metric_results(id)
);
```

---

## 6. 配置管理

### 6.1 环境变量

创建 `.env` 文件：

```bash
# LLM 配置
LLM_PROVIDER=claude
LLM_MODEL=claude-sonnet-4-5-20250929
LLM_API_KEY=your-api-key

# Azure OpenAI 配置（可选）
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT=your-deployment-name
AZURE_OPENAI_API_VERSION=2024-02-01

# Snowflake 配置
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ENT_HACKATHON_DATA_SHARE
SNOWFLAKE_SCHEMA=EA_HACKATHON
SNOWFLAKE_ROLE=your-role

# 应用配置
APP_NAME=AZ Data Agent
DEBUG=false
LOG_LEVEL=INFO
```

### 6.2 主配置文件 (config/settings.yaml)

```yaml
llm:
  provider: "${LLM_PROVIDER:claude}"
  model: "${LLM_MODEL:claude-sonnet-4-5-20250929}"

database:
  type: "snowflake"
  account: "${SNOWFLAKE_ACCOUNT}"
  user: "${SNOWFLAKE_USER}"
  warehouse: "${SNOWFLAKE_WAREHOUSE:COMPUTE_WH}"
  database: "${SNOWFLAKE_DATABASE:ENT_HACKATHON_DATA_SHARE}"
  schema: "${SNOWFLAKE_SCHEMA:EA_HACKATHON}"

monitor:
  interval_minutes: 5
  metrics_config: "config/metrics_template.yaml"
  enabled: true

app:
  name: "AZ Data Agent"
  debug: false
```

### 6.3 指标模板配置 (config/metrics_template.yaml)

```yaml
metrics:
  - name: "Revenue Budget Variance"
    description: "Percentage variance between actual and budget revenue"
    category: "variance"
    sql_template: |
      SELECT
        SUM(f.BUD_VARIANCE) / NULLIF(SUM(f.BUD_VALUE), 0) * 100 as variance_pct
      FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
      JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t
        ON f.TIME_KEY = t.TIME_KEY
      WHERE t.IS_CURRENT_QUARTER = TRUE
    threshold_type: "percentage"
    threshold_value: 10.0
    threshold_operator: "gt"

  - name: "AZ Market Share"
    description: "AstraZeneca market share percentage"
    category: "market_share"
    sql_template: |
      SELECT
        SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) /
        NULLIF(SUM(f.VALUE), 0) * 100 as az_share
      FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
      JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p
        ON f.PRODUCT_KEY = p.PRODUCT_KEY
      WHERE t.IS_CURRENT_YEAR = TRUE
    threshold_type: "percentage"
    threshold_value: 20.0
    threshold_operator: "lt"

  - name: "YoY Revenue Change"
    description: "Year-over-year revenue change percentage"
    category: "variance"
    sql_template: |
      SELECT
        SUM(f.PY_VARIANCE) / NULLIF(SUM(f.PY_VALUE), 0) * 100 as yoy_change
      FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
      JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t
        ON f.TIME_KEY = t.TIME_KEY
      WHERE t.IS_CURRENT_YEAR = TRUE
    threshold_type: "percentage"
    threshold_value: -15.0
    threshold_operator: "lt"
```

---

## 7. 部署指南

### 7.1 环境要求

| 要求 | 说明 |
|------|------|
| Python | 3.10 或更高版本 |
| pip | Python 包管理器 |
| Snowflake | 有效的 Snowflake 账号 |
| LLM API | Claude/OpenAI/Azure OpenAI API 密钥 |

### 7.2 安装步骤

```bash
# 1. 克隆项目
git clone <repository-url>
cd az-data-agent

# 2. 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# 3. 安装依赖
pip install -e .

# 4. 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入配置

# 5. 初始化数据库
python -c "from src.monitor.models import init_database; init_database()"

# 6. 加载指标模板
python -c "from src.monitor.metrics_loader import load_metrics_from_yaml; load_metrics_from_yaml()"
```

### 7.3 启动服务

#### 7.3.1 启动 Web UI

```bash
streamlit run src/web/app.py
```

访问: http://localhost:8501

#### 7.3.2 启动监控服务

```bash
python -m src.monitor.scheduler
```

#### 7.3.3 同时启动（推荐）

在 Web 应用中集成监控服务：

```python
# 在 app.py 中
from src.monitor.scheduler import start_monitor_service

# 启动监控服务
start_monitor_service()
```

### 7.4 生产部署建议

#### 7.4.1 使用 Gunicorn + Streamlit

```bash
pip install gunicorn
gunicorn src.web.app:app -w 4 -b 0.0.0.0:8501
```

#### 7.4.2 使用 Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN pip install -e .

EXPOSE 8501

CMD ["streamlit", "run", "src/web/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
docker build -t az-data-agent .
docker run -p 8501:8501 --env-file .env az-data-agent
```

#### 7.4.3 监控服务作为独立进程

```bash
# 使用 systemd (Linux)
# 创建 /etc/systemd/system/az-monitor.service
[Unit]
Description=AZ Data Agent Monitor Service

[Service]
Type=simple
User=appuser
WorkingDirectory=/path/to/az-data-agent
ExecStart=/path/to/.venv/bin/python -m src.monitor.scheduler
Restart=always

[Install]
WantedBy=multi-user.target

# 启用服务
sudo systemctl enable az-monitor
sudo systemctl start az-monitor
```

---

## 8. 开发规范

### 8.1 代码风格

| 工具 | 配置 |
|------|------|
| **Black** | line-length = 88, target-version = py310 |
| **isort** | profile = "black" |
| **mypy** | 静态类型检查 |

```bash
# 格式化代码
black src/ && isort src/

# 类型检查
mypy src/
```

### 8.2 类型注解

所有函数必须包含类型注解：

```python
# 正确
def process_data(data: list[dict]) -> dict[str, Any]:
    ...

# 错误
def process_data(data):
    ...
```

### 8.3 文档字符串

使用 Google 风格的文档字符串：

```python
def calculate_variance(actual: float, budget: float) -> float:
    """
    Calculate variance percentage.

    Args:
        actual: The actual value.
        budget: The budget value.

    Returns:
        Variance percentage.

    Raises:
        ValueError: If budget is zero.
    """
    if budget == 0:
        raise ValueError("Budget cannot be zero")
    return (actual - budget) / budget * 100
```

### 8.4 导入顺序

按照 isort 标准顺序：

```python
# 1. 标准库
import os
from typing import Optional

# 2. 第三方库
import pandas as pd
from langchain.agents import AgentExecutor

# 3. 本地模块
from src.core.config import Settings
from src.core.database import execute_query
```

### 8.5 配置管理规范

- 使用 Pydantic Settings 管理配置
- 敏感信息通过环境变量传递
- 不在代码中硬编码配置值

### 8.6 数据库查询规范

- 使用参数化查询防止 SQL 注入
- 使用完整的表路径: `ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>`
- 查询结果限制行数，避免内存溢出

---

## 9. 测试指南

### 9.1 测试结构

```
tests/
├── conftest.py              # 测试配置和 fixtures
├── test_core/               # 核心模块测试
│   ├── test_config.py
│   ├── test_database.py
│   ├── test_llm_provider.py
│   └── test_integration.py
├── test_monitor/            # 监控模块测试
│   └── __init__.py
└── test_agent/              # Agent 模块测试
    └── __init__.py
```

### 9.2 运行测试

```bash
# 运行所有测试
pytest tests/

# 运行特定模块测试
pytest tests/test_core/

# 运行单个测试文件
pytest tests/test_core/test_config.py

# 显示覆盖率
pytest --cov=src tests/

# 详细输出
pytest -v tests/
```

### 9.3 测试配置 (conftest.py)

```python
import pytest
from src.core.config import Settings, reset_settings

@pytest.fixture
def test_settings():
    """创建测试用配置"""
    reset_settings()
    return Settings(
        llm_provider="claude",
        llm_api_key="test-key",
        snowflake_account="test-account",
        snowflake_user="test-user",
        snowflake_password="test-password",
    )
```

### 9.4 测试示例

```python
# tests/test_core/test_config.py
from src.core.config import Settings, get_settings, reset_settings

def test_settings_defaults():
    """测试默认配置"""
    reset_settings()
    settings = Settings()
    assert settings.llm_provider == "claude"
    assert settings.monitor_interval_minutes == 5

def test_settings_from_env(monkeypatch):
    """测试从环境变量加载配置"""
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-4")
    reset_settings()
    settings = Settings()
    assert settings.llm_provider == "openai"
    assert settings.llm_model == "gpt-4"
```

---

## 附录

### A. 常见问题

**Q: 如何切换 LLM 提供商？**

A: 修改 `.env` 文件中的 `LLM_PROVIDER` 值为 `claude`、`openai` 或 `azure`。

**Q: 如何添加新的监控指标？**

A: 在 `config/metrics_template.yaml` 中添加新的指标定义，或通过代码直接插入 SQLite 数据库。

**Q: 如何扩展 Agent 能力？**

A: 创建新的 Tool 类继承 `BaseTool`，然后在 `create_data_agent` 函数中添加到工具列表。

### B. 相关文档

- 设计文档: `docs/superpowers/specs/2026-03-16-az-data-agent-design.md`
- 实现计划: `docs/superpowers/plans/2026-03-16-az-data-agent-implementation.md`
- 数据模型: `000_客户提供的资料/HACKATHON_Data_Model_v2.md`
- 数据字典: `000_客户提供的资料/HACKATHON_Data_Dictionary_v2.md`

### C. 参考资料

- [LangChain 文档](https://python.langchain.com/docs/)
- [Streamlit 文档](https://docs.streamlit.io/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector)
- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)