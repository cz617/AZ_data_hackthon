# AZ Data Agent - 设计文档

**创建日期**: 2026-03-16
**项目类型**: Hackathon 项目
**技术栈**: Python, LangChain, Snowflake, Streamlit

---

## 1. 项目概述

### 1.1 背景

基于 AstraZeneca 医药数据构建 AI Agent 数据分析系统，支持：
- **智能监控**: 定时执行 SQL 监控指标，自动检测异常
- **AI 分析**: LangChain Agent 自动分析数据，回答用户问题
- **自动告警**: 异常触发时自动调用 Agent 进行深度分析

### 1.2 核心需求

| 需求 | 描述 |
|------|------|
| 监控配置 | 预定义业务指标模板 + 用户自定义 SQL |
| 定时执行 | APScheduler 定时执行 SQL 并判断阈值 |
| 告警触发 | 超过阈值自动触发 Agent 分析 |
| Agent 交互 | 支持用户直接问答 + 监控自动触发分析 |
| Web UI | Streamlit 实现 Web 交互界面 |

---

## 2. 架构设计

### 2.1 模块化服务架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AZ Data Agent System                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐        │
│  │   Web UI     │     │   Monitor    │     │  Data Agent  │        │
│  │  (Streamlit) │     │   Service    │     │   Service    │        │
│  │  Port: 8501  │     │  Background  │     │  On-Demand   │        │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘        │
│         │                    │                    │                 │
│         └────────────────────┴────────────────────┘                 │
│                              │                                       │
│                   ┌──────────┴──────────┐                           │
│                   │   Shared Services   │                           │
│                   │  - Config Manager   │                           │
│                   │  - LLM Provider     │                           │
│                   │  - Snowflake Client │                           │
│                   │  - SQLite Queue     │                           │
│                   └─────────────────────┘                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 技术选型

| 组件 | 技术选择 | 理由 |
|------|---------|------|
| LLM 框架 | LangChain + Middleware + Skills | 支持 middleware 扩展，skills 按需加载 |
| LLM 模型 | 可配置（Claude/OpenAI/Azure） | 灵活适配不同环境 |
| 数据库 | Snowflake | 客户现有数据仓库 |
| 任务队列 | SQLite | 简单轻量，无需额外服务 |
| Web UI | Streamlit | 快速开发，适合数据应用 |
| 定时任务 | APScheduler | Python 内置，简单可靠 |

---

## 3. 项目结构

```
az-data-agent/
├── src/
│   ├── core/                    # 核心共享模块
│   │   ├── __init__.py
│   │   ├── config.py           # 配置管理
│   │   ├── database.py         # Snowflake 连接池
│   │   └── llm_provider.py     # LLM 抽象层
│   │
│   ├── monitor/                 # 监控服务模块
│   │   ├── __init__.py
│   │   ├── scheduler.py        # APScheduler 定时任务
│   │   ├── metrics_config.py   # 指标配置表模型
│   │   ├── executor.py         # SQL 执行器
│   │   └── alert_engine.py     # 告警判断与触发
│   │
│   ├── agent/                   # Data Agent 模块
│   │   ├── __init__.py
│   │   ├── agent.py            # LangChain Agent 主程序
│   │   ├── middleware/         # 中间件
│   │   │   ├── __init__.py
│   │   │   ├── skill_loader.py
│   │   │   └── context_enricher.py
│   │   ├── skills/             # Skills 能力模块
│   │   │   ├── __init__.py
│   │   │   ├── sql_analyzer.py
│   │   │   ├── data_visualizer.py
│   │   │   └── report_generator.py
│   │   └── tools/              # Agent Tools
│   │       ├── __init__.py
│   │       ├── snowflake_tool.py
│   │       └── analysis_tool.py
│   │
│   ├── web/                     # Web UI 模块
│   │   ├── __init__.py
│   │   ├── app.py              # Streamlit 主入口
│   │   ├── pages/
│   │   │   ├── 1_chat.py       # 对话页面
│   │   │   ├── 2_monitor.py    # 监控配置页面
│   │   │   └── 3_history.py    # 历史记录页面
│   │   └── components/
│   │       ├── chat_box.py
│   │       └── metric_card.py
│   │
│   └── messaging/               # 消息队列模块
│       ├── __init__.py
│       ├── queue.py            # SQLite 队列实现
│       └── models.py           # 消息模型
│
├── config/                      # 配置文件
│   ├── settings.yaml           # 主配置
│   ├── metrics_template.yaml   # 预定义指标模板
│   └── prompts/                # Agent 提示词
│       └── system_prompt.md
│
├── tests/                       # 测试
│   ├── test_monitor/
│   ├── test_agent/
│   └── test_integration/
│
├── docs/                        # 文档
│   └── superpowers/
│       └── specs/
│
├── scripts/                     # 脚本
│   ├── start_all.sh            # 启动所有服务
│   └── init_db.py              # 初始化数据库
│
├── pyproject.toml              # 项目配置
├── requirements.txt
└── README.md
```

---

## 4. 核心组件设计

### 4.1 配置管理

```python
# src/core/config.py
from pydantic_settings import BaseSettings
from typing import Literal

class Settings(BaseSettings):
    # LLM 配置
    llm_provider: Literal["claude", "openai", "azure"] = "claude"
    llm_model: str = "claude-sonnet-4-5-20250929"
    llm_api_key: str = ""

    # Snowflake 配置
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = ""
    snowflake_database: str = "ENT_HACKATHON_DATA_SHARE"
    snowflake_schema: str = "EA_HACKATHON"

    # 监控配置
    monitor_interval_minutes: int = 5

    class Config:
        env_file = ".env"
```

### 4.2 监控指标配置表 (SQLite)

```sql
-- 指标定义表
CREATE TABLE metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    category TEXT,                         -- revenue, market_share, variance
    sql_template TEXT NOT NULL,           -- SQL 模板
    threshold_type TEXT NOT NULL,         -- absolute | percentage | change
    threshold_value REAL NOT NULL,
    threshold_operator TEXT NOT NULL,     -- gt | lt | eq | gte | lte
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
    status TEXT DEFAULT 'pending',        -- pending | processing | completed
    analysis_result TEXT,                 -- Agent 分析结果
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    FOREIGN KEY (metric_id) REFERENCES metrics(id),
    FOREIGN KEY (result_id) REFERENCES metric_results(id)
);
```

### 4.3 预定义指标模板

| 指标名称 | SQL 模板 | 阈值类型 | 阈值 |
|---------|----------|---------|------|
| 收入预算偏差率 | `SELECT SUM(BUD_VARIANCE)/NULLIF(SUM(BUD_VALUE),0)*100 ...` | percentage | 10% |
| 市场份额变化 | `SELECT ... FROM FACT_COM_BASE_BRAND ...` | change | -5% |
| YoY 收入下降 | `SELECT PY_VARIANCE/NULLIF(PY_VALUE,0)*100 ...` | percentage | -15% |

---

## 5. Agent 架构设计

### 5.1 Agent 组件层次

```
┌─────────────────────────────────────────────────────────────┐
│                      Data Analysis Agent                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                    Middleware Stack                   │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐  │   │
│  │  │SkillLoader  │→│ContextEnrich│→│OutputFormatter │  │   │
│  │  │Middleware   │ │erMiddleware │ │Middleware      │  │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
│                           ↓                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                      Skills Pool                      │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐  │   │
│  │  │SQLAnalyzer  │ │DataVizSkill │ │ReportGenerator │  │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
│                           ↓                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                      Tools Layer                      │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐  │   │
│  │  │SnowflakeTool│ │ChartTool    │ │ExportTool      │  │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘  │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 Middleware 实现

```python
# src/agent/middleware/skill_loader.py
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse

class SkillLoaderMiddleware(AgentMiddleware):
    """按需加载 Skills，注入到 Agent 上下文"""

    def __init__(self, skills_registry: dict):
        self.skills_registry = skills_registry

    def wrap_model_call(self, request: ModelRequest, handler) -> ModelResponse:
        # 根据用户问题判断需要加载哪些 skills
        relevant_skills = self._select_skills(request.messages)

        # 将 skill 描述注入 system prompt
        skill_descriptions = self._build_skill_prompt(relevant_skills)
        modified_request = request.override(
            system_prompt=request.system_prompt + skill_descriptions
        )
        return handler(modified_request)
```

### 5.3 Skills 定义

```python
# src/agent/skills/sql_analyzer.py
class SQLAnalyzerSkill:
    """SQL 分析技能"""
    name = "sql_analyzer"
    description = "分析数据需求，生成 Snowflake SQL 查询"

# src/agent/skills/data_visualizer.py
class DataVisualizerSkill:
    """数据可视化技能"""
    name = "data_visualizer"
    description = "将查询结果转换为可视化图表"

# src/agent/skills/report_generator.py
class ReportGeneratorSkill:
    """报告生成技能"""
    name = "report_generator"
    description = "生成数据分析报告"
```

### 5.4 Agent 主程序

```python
# src/agent/agent.py
from langchain.agents import create_agent

def create_data_agent(llm_provider: str):
    """创建数据分析 Agent"""

    model = get_llm_model(llm_provider)
    tools = [SnowflakeTool(), ChartTool(), ExportTool()]
    middleware = [
        SkillLoaderMiddleware(SKILLS_REGISTRY),
        ContextEnricherMiddleware(DATA_CONTEXT),
    ]

    agent = create_agent(
        model=model,
        tools=tools,
        middleware=middleware,
        system_prompt=SYSTEM_PROMPT,
    )

    return agent
```

---

## 6. 数据流设计

### 6.1 用户交互流程

```
用户 → Web UI → Agent → Snowflake
                ↓
            图表生成 (Plotly)
                ↓
            结果展示
```

### 6.2 监控告警流程

```
Scheduler → Executor → Alert Engine → Alert Queue
                           ↓
                      触发 Agent 分析
                           ↓
                      存储分析结果
                           ↓
                      Web UI 展示
```

---

## 7. 配置文件

### 7.1 主配置 (config/settings.yaml)

```yaml
llm:
  provider: "claude"
  model: "claude-sonnet-4-5-20250929"

database:
  type: "snowflake"
  account: "${SNOWFLAKE_ACCOUNT}"
  user: "${SNOWFLAKE_USER}"
  warehouse: "COMPUTE_WH"
  database: "ENT_HACKATHON_DATA_SHARE"
  schema: "EA_HACKATHON"

monitor:
  interval_minutes: 5
  metrics_config: "config/metrics_template.yaml"
```

### 7.2 指标模板配置 (config/metrics_template.yaml)

```yaml
metrics:
  - name: "收入预算偏差率"
    category: "variance"
    sql_template: |
      SELECT
        SUM(f.BUD_VARIANCE) / NULLIF(SUM(f.BUD_VALUE), 0) * 100 as variance_pct
      FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
      JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t
        ON f.TIME_KEY = t.TIME_KEY
      WHERE t.IS_CURRENT_QUARTER = TRUE
    threshold_type: "percentage"
    threshold_value: 10
    threshold_operator: "gt"

  - name: "市场份额下降"
    category: "market_share"
    sql_template: |
      SELECT
        SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) /
        NULLIF(SUM(f.VALUE), 0) * 100 as az_share
      FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
      JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p
        ON f.PRODUCT_KEY = p.PRODUCT_KEY
    threshold_type: "change"
    threshold_value: -5
    threshold_operator: "lt"
```

---

## 8. 部署与启动

### 8.1 环境要求

- Python 3.10+
- Snowflake 账号
- LLM API Key (Claude/OpenAI/Azure)

### 8.2 启动命令

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
export SNOWFLAKE_ACCOUNT=xxx
export SNOWFLAKE_USER=xxx
export SNOWFLAKE_PASSWORD=xxx
export LLM_API_KEY=xxx

# 初始化数据库
python scripts/init_db.py

# 启动所有服务
./scripts/start_all.sh
```

### 8.3 访问地址

- Web UI: http://localhost:8501

---

## 9. 扩展计划

### Phase 1 (当前)
- 基础监控功能
- Agent 问答能力
- Web UI 交互

### Phase 2 (后续)
- 增加更多 Skills
- 支持更多 LLM 提供商
- 优化监控指标模板

### Phase 3 (未来)
- 多用户支持
- 权限管理
- API 接口开放

---

## 附录

### A. 数据模型参考

详见客户提供的资料:
- `000_客户提供的资料/HACKATHON_Data_Model_v2.md`
- `000_客户提供的资料/HACKATHON_Data_Dictionary_v2.md`

### B. LangChain Middleware 参考

- https://docs.langchain.com/oss/python/langchain/middleware/custom
- https://docs.langchain.com/oss/python/langchain/multi-agent/skills-sql-assistant