# AZ Data Agent - DeepAgent 架构设计

**创建日期**: 2026-03-16
**版本**: 2.0
**技术栈**: LangChain 1.0, DeepAgent, Snowflake, Streamlit

---

## 1. 概述

### 1.1 背景

基于 LangChain 1.0 DeepAgent 架构重新设计 Agent 模块，充分利用 DeepAgent 的内置功能（自动压缩、文件系统、Skills 支持），提升数据分析能力。

### 1.2 核心变更

| 变更项 | 旧方案 | 新方案 |
|--------|--------|--------|
| Agent 框架 | LangChain 0.3 `create_tool_calling_agent` | DeepAgent `create_deep_agent` |
| Skills | 自定义实现 | DeepAgent 原生 Skills 文件系统 |
| Middleware | 自定义实现 | LangChain AgentMiddleware 接口 |
| 依赖包 | langchain>=0.3.0 | langchain>=0.3.0, deepagents>=0.1.0 |

**注意**: `deepagents` 是 LangChain 官方的 pre-1.0 包，与 LangChain 0.3+ 兼容。

### 1.3 关键决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| Skills | SQL 分析 + 可视化 + 报告 | 完整数据分析能力 |
| Middleware | 数据上下文 + 告警触发 | 注入上下文 + 监控集成 |
| SubAgent | 不使用 | 任务连贯，不需要隔离 |
| 告警集成 | 监控主动推送 | 简单直接，易于控制 |
| 上下文来源 | 硬编码配置 | 快速开发，内容稳定 |

---

## 2. 整体架构

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AZ Data Agent (DeepAgent)                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        Middleware Layer                             │ │
│  │  ┌──────────────────────────────────────────────────────────────┐  │ │
│  │  │ DataContextMiddleware                                        │  │ │
│  │  │ • 表结构注入 • 业务元数据注入                                 │  │ │
│  │  └──────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    ↓                                     │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                          Tools Layer                                │ │
│  │  ┌──────────────────────┐  ┌───────────────────────────────────┐   │ │
│  │  │ snowflake_query      │  │ create_chart                      │   │ │
│  │  │ • 执行 SQL 查询       │  │ • 生成 Plotly 图表                │   │ │
│  │  └──────────────────────┘  └───────────────────────────────────┘   │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                    ↓                                     │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                          Skills Layer                               │ │
│  │  ┌────────────────┐  ┌────────────────┐  ┌─────────────────────┐   │ │
│  │  │ sql_analyzer   │  │ data_visualizer│  │ report_generator    │   │ │
│  │  │ • SQL 生成      │  │ • 图表选择      │  │ • 报告结构          │   │ │
│  │  │ • 查询模板      │  │ • 可视化指南    │  │ • 分析模板          │   │ │
│  │  └────────────────┘  └────────────────┘  └─────────────────────┘   │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ↓
                    ┌────────────────────────────────┐
                    │      External Integrations      │
                    │  ┌─────────────┐ ┌───────────┐ │
                    │  │ Snowflake   │ │ Monitor   │ │
                    │  │ Data        │ │ Service   │ │
                    │  │             │ │ +AlertTr- │ │
                    │  │             │ │ iggerHandl│ │
                    │  └─────────────┘ └───────────┘ │
                    └────────────────────────────────┘
```

### 2.2 模块结构

```
src/agent/
├── __init__.py                    # 模块导出
├── agent.py                       # DeepAgent 主入口
│
├── middleware/                    # 中间件
│   ├── __init__.py
│   ├── data_context.py           # 数据上下文中间件
│   └── alert_trigger.py          # 告警触发中间件
│
├── skills/                        # 技能模块
│   ├── __init__.py
│   ├── sql_analyzer/             # SQL 分析技能
│   │   └── skill.md
│   ├── data_visualizer/          # 数据可视化技能
│   │   └── skill.md
│   └── report_generator/         # 报告生成技能
│       └── skill.md
│
├── tools/                         # 工具
│   ├── __init__.py
│   ├── snowflake_tool.py         # Snowflake 查询工具
│   └── chart_tool.py             # 图表生成工具
│
└── context/                       # 上下文配置
    ├── __init__.py
    └── business_context.py       # 硬编码的业务元数据
```

### 2.3 依赖更新

```toml
# pyproject.toml
dependencies = [
    "langchain>=0.3.0",           # LangChain 核心
    "deepagents>=0.1.0",          # DeepAgent 包 (pre-1.0)
    "langchain-anthropic>=0.3.0",
    "langchain-openai>=0.3.0",
    "snowflake-connector-python>=3.0.0",
    "streamlit>=1.30.0",
    "apscheduler>=3.10.0",
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    "sqlalchemy>=2.0.0",
    "pyyaml>=6.0",
    "plotly>=5.0.0",
    "pandas>=2.0.0",
    "python-dotenv>=1.0.0",
]
```

**注意**: `deepagents` 是 LangChain 官方的 pre-1.0 包，API 可能在 minor 版本间变化。

---

## 3. Middleware 中间件

### 3.1 数据上下文中间件

**文件**: `src/agent/middleware/data_context.py`

**职责**: 注入 Snowflake 表结构、字段说明、业务元数据到 Agent 上下文。

```python
from typing import Callable
from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain.messages import SystemMessage

from src.agent.context.business_context import BUSINESS_CONTEXT


class DataContextMiddleware(AgentMiddleware):
    """
    数据上下文中间件

    通过 wrap_model_call 钩子在每次 LLM 调用前注入业务上下文。
    将业务元数据追加到 system_message 的 content_blocks 中。
    """

    def __init__(self):
        super().__init__()
        self.context = BUSINESS_CONTEXT

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        """
        在请求中注入上下文

        将业务上下文追加到 system_message 的 content_blocks 中。
        """
        # 获取当前 system_message 的 content_blocks
        current_blocks = list(request.system_message.content_blocks)

        # 添加业务上下文
        context_block = {
            "type": "text",
            "text": f"\n\n## 数据仓库上下文\n\n{self.context}"
        }
        new_blocks = current_blocks + [context_block]

        # 创建新的 system_message
        new_system_message = SystemMessage(content=new_blocks)

        # 使用修改后的请求调用 handler
        return handler(request.override(system_message=new_system_message))
```

### 3.2 业务上下文配置

**文件**: `src/agent/context/business_context.py`

```python
BUSINESS_CONTEXT = """
## Snowflake 数据仓库

### 数据库路径
ENT_HACKATHON_DATA_SHARE.EA_HACKATHON

### 事实表

#### FACT_PNL_BASE_BRAND (P&L 财务数据)
| 字段 | 类型 | 说明 |
|------|------|------|
| BUD_VALUE | NUMBER | 预算值 |
| BUD_VARIANCE | NUMBER | 预算偏差 |
| PY_VALUE | NUMBER | 去年同期值 |
| PY_VARIANCE | NUMBER | 同比偏差 |

#### FACT_COM_BASE_BRAND (商业/市场数据)
| 字段 | 类型 | 说明 |
|------|------|------|
| VALUE | NUMBER | 市场值 |
| MARKET_SHARE | NUMBER | 市场份额 |

### 维度表

- DIM_ACCOUNT: P&L 科目维度
- DIM_PRODUCT: 产品/品牌维度（含 AZ_PROD_IND 标识阿斯利康产品）
- DIM_MARKET: 治疗市场维度
- DIM_TIME: 时间维度（含 IS_CURRENT_QUARTER, IS_CURRENT_YEAR 标识）
- DIM_SCENARIO: 计划场景

### 地理范围
- Spain (44000ES)
- Brazil (44000BR)

### 治疗领域
- Oncology TA
- BioPharma TA (CVRM)
- Rare Disease TA
- Central TA
"""
```

### 3.3 告警触发处理器

**文件**: `src/agent/middleware/alert_trigger.py`

**职责**: 接收监控模块的告警推送，调用 Agent 进行自动分析。

**设计说明**:

这是一个**事件驱动的回调处理器**，而非 AgentMiddleware 的实现：
- 它不参与 Agent 的请求处理流程（不实现 `wrap_model_call`）
- 作为独立的组件，持有 Agent 的 `invoke` 方法引用
- 监控模块通过 `get_alert_handler()` 获取实例并调用 `on_alert()`
- 命名为 `AlertTriggerHandler` 而非 `Middleware` 以避免混淆

```python
from typing import Callable, Optional
from datetime import datetime

from src.monitor.models import AlertQueue, Metric, MetricResult, AlertStatus


class AlertTriggerHandler:
    """
    处理监控告警的触发和回调

    这是一个事件驱动的处理器，不参与 Agent 的请求处理流程。
    监控模块检测到告警后，调用此处理器的 on_alert() 方法触发 Agent 分析。
    """

    def __init__(self):
        self._agent_invoke: Optional[Callable] = None

    def set_agent_invoke(self, invoke_func: Callable):
        """
        设置 Agent 的 invoke 方法

        必须在 Web 应用启动时调用此方法注册 Agent 回调。
        """
        self._agent_invoke = invoke_func

    def on_alert(
        self,
        alert: AlertQueue,
        metric: Metric,
        result: MetricResult,
    ) -> str:
        """
        处理告警事件

        Args:
            alert: 告警队列记录
            metric: 触发的指标
            result: 执行结果

        Returns:
            分析结果
        """
        if not self._agent_invoke:
            return "Agent not initialized"

        prompt = self._build_analysis_prompt(metric, result)

        try:
            response = self._agent_invoke({
                "messages": [{"role": "user", "content": prompt}]
            })
            return response.get("output", "Analysis completed")
        except Exception as e:
            return f"Analysis failed: {str(e)}"

    def _build_analysis_prompt(
        self,
        metric: Metric,
        result: MetricResult,
    ) -> str:
        """构建分析提示词"""
        return f"""
## 监控告警分析请求

### 告警信息
- **指标名称**: {metric.name}
- **指标描述**: {metric.description}
- **指标类别**: {metric.category}

### 触发数据
- **当前值**: {result.actual_value}
- **阈值条件**: {metric.threshold_operator} {metric.threshold_value}
- **触发时间**: {result.executed_at}

### 分析要求

请执行以下分析：

1. **异常确认**: 查询相关数据确认异常情况
2. **原因分析**: 分析导致异常的可能原因
3. **影响评估**: 评估对业务的影响范围
4. **建议措施**: 提供具体的业务建议

请生成一份完整的分析报告。
"""


# 全局单例
_alert_handler: Optional[AlertTriggerHandler] = None


def get_alert_handler() -> AlertTriggerHandler:
    """获取告警处理器单例"""
    global _alert_handler
    if _alert_handler is None:
        _alert_handler = AlertTriggerHandler()
    return _alert_handler
```

---

## 4. Skills 技能模块

### 4.1 Skills 架构

```
src/agent/skills/
├── __init__.py
├── sql_analyzer/              # SQL 分析技能
│   └── skill.md
├── data_visualizer/          # 数据可视化技能
│   └── skill.md
└── report_generator/         # 报告生成技能
    └── skill.md
```

### 4.2 SQL 分析技能

**文件**: `src/agent/skills/sql_analyzer/skill.md`

```markdown
# SQL Analyzer Skill

## 描述
分析用户的数据需求，生成正确的 Snowflake SQL 查询。

## 能力
- 理解自然语言的数据查询需求
- 生成符合 Snowflake 语法的 SQL
- 正确使用 JOIN 连接事实表和维度表
- 处理 NULL 值和除零保护

## 使用场景
- 用户询问收入、成本等财务指标
- 用户需要按产品、市场、时间等维度分析
- 用户需要同比、环比、预算偏差分析

## 查询模板

### 当前季度收入
```sql
SELECT SUM(f.BUD_VALUE) as revenue
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t
  ON f.TIME_KEY = t.TIME_KEY
WHERE t.IS_CURRENT_QUARTER = TRUE
```

### 按产品类别的预算偏差
```sql
SELECT
  p.PRODUCT_NAME,
  SUM(f.BUD_VARIANCE) / NULLIF(SUM(f.BUD_VALUE), 0) * 100 as variance_pct
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p
  ON f.PRODUCT_KEY = p.PRODUCT_KEY
GROUP BY p.PRODUCT_NAME
ORDER BY variance_pct DESC
```

### AZ 市场份额
```sql
SELECT
  SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) /
  NULLIF(SUM(f.VALUE), 0) * 100 as az_share
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p
  ON f.PRODUCT_KEY = p.PRODUCT_KEY
```

## 注意事项
1. 始终使用完整表路径
2. 使用 NULLIF 防止除零错误
3. 限制结果行数（建议 TOP 100）
```

### 4.3 数据可视化技能

**文件**: `src/agent/skills/data_visualizer/skill.md`

```markdown
# Data Visualizer Skill

## 描述
将查询结果转换为可视化图表，支持多种图表类型。

## 能力
- 根据数据特征自动推荐图表类型
- 生成 Plotly 兼容的图表配置
- 支持柱状图、折线图、饼图、散点图

## 图表选择指南

| 数据类型 | 推荐图表 |
|---------|---------|
| 分类对比 | bar (柱状图) |
| 时间趋势 | line (折线图) |
| 占比分析 | pie (饼图) |
| 相关性分析 | scatter (散点图) |

## 使用示例

### 柱状图
```json
{
  "type": "bar",
  "x": ["Product A", "Product B", "Product C"],
  "y": [100, 150, 80],
  "title": "Revenue by Product"
}
```

### 折线图
```json
{
  "type": "line",
  "x": ["Q1", "Q2", "Q3", "Q4"],
  "y": [100, 120, 115, 140],
  "title": "Quarterly Revenue Trend"
}
```

## 工具调用
使用 `create_chart` 工具生成图表。
```

### 4.4 报告生成技能

**文件**: `src/agent/skills/report_generator/skill.md`

```markdown
# Report Generator Skill

## 描述
生成结构化的数据分析报告。

## 能力
- 整合多维度分析结果
- 生成 Markdown 格式报告
- 包含数据表格和图表引用

## 报告模板

### 数据分析报告结构
```markdown
# [报告标题]

## 概述
[简要描述分析目的和主要发现]

## 数据摘要
| 指标 | 数值 |
|------|------|
| ... | ... |

## 详细分析
[分析内容和图表]

## 结论与建议
[基于数据的业务建议]

## 数据来源
- 查询时间: [timestamp]
- 数据范围: [scope]
```

## 使用场景
- 监控告警的自动分析报告
- 用户请求的综合分析报告
- 定期业务分析报告
```

### 4.5 Skills 注册与加载机制

**文件**: `src/agent/skills/__init__.py`

```python
from pathlib import Path

SKILLS_DIR = Path(__file__).parent

SKILLS_REGISTRY = {
    "sql_analyzer": str(SKILLS_DIR / "sql_analyzer"),
    "data_visualizer": str(SKILLS_DIR / "data_visualizer"),
    "report_generator": str(SKILLS_DIR / "report_generator"),
}


def get_skill_paths() -> list[str]:
    """获取所有技能目录路径，供 DeepAgent 加载"""
    return list(SKILLS_REGISTRY.values())
```

**Skills 加载流程**:

1. `create_deep_agent()` 接收 `skills` 参数（**技能目录路径列表**，非文件路径）
2. DeepAgent 内部查找每个目录下的 `skill.md` 文件
3. 将 `skill.md` 的内容注入到 Agent 的上下文中
4. Agent 在处理请求时，可以根据 skill 中的指南生成更好的响应

**skill.md 文件格式要求**:
- 必须包含技能描述和使用场景
- 可包含查询模板、示例代码
- 使用 Markdown 格式，便于 Agent 解析

**目录结构示例**:
```
src/agent/skills/sql_analyzer/
└── skill.md              # 必须存在
```

---

## 5. Tools 工具层

### 5.1 Snowflake 查询工具

**文件**: `src/agent/tools/snowflake_tool.py`

```python
from typing import Type
from pydantic import BaseModel, Field

from src.core.database import execute_query_with_columns
from src.core.config import get_settings


class SnowflakeToolInput(BaseModel):
    """Snowflake 工具输入"""
    sql: str = Field(
        description="要执行的 Snowflake SQL 查询语句"
    )


def snowflake_query(sql: str) -> str:
    """
    执行 Snowflake SQL 查询

    Args:
        sql: SQL 查询语句

    Returns:
        查询结果的格式化字符串
    """
    settings = get_settings()

    try:
        columns, rows = execute_query_with_columns(sql, settings)

        if not rows:
            return "查询没有返回结果。"

        # 格式化输出
        result = f"列名: {', '.join(columns)}\n\n"
        result += f"共 {len(rows)} 行数据:\n\n"

        # 显示前 20 行
        for i, row in enumerate(rows[:20]):
            row_str = " | ".join(str(v) if v is not None else "NULL" for v in row)
            result += f"{i+1}. {row_str}\n"

        if len(rows) > 20:
            result += f"\n... 还有 {len(rows) - 20} 行数据"

        return result

    except Exception as e:
        return f"查询执行失败: {str(e)}"
```

### 5.2 图表生成工具

**文件**: `src/agent/tools/chart_tool.py`

```python
import json
from typing import Literal
from pydantic import BaseModel, Field


class ChartToolInput(BaseModel):
    """图表工具输入"""
    chart_type: Literal["bar", "line", "pie", "scatter"] = Field(
        description="图表类型: bar(柱状图), line(折线图), pie(饼图), scatter(散点图)"
    )
    x: list = Field(
        description="X 轴数据（分类标签或数值）"
    )
    y: list = Field(
        description="Y 轴数据（数值）"
    )
    title: str = Field(
        default="数据可视化",
        description="图表标题"
    )
    x_label: str = Field(
        default="",
        description="X 轴标签"
    )
    y_label: str = Field(
        default="",
        description="Y 轴标签"
    )


def create_chart(
    chart_type: str,
    x: list,
    y: list,
    title: str = "数据可视化",
    x_label: str = "",
    y_label: str = "",
) -> str:
    """
    创建数据可视化图表

    Args:
        chart_type: 图表类型
        x: X 轴数据
        y: Y 轴数据
        title: 图表标题
        x_label: X 轴标签
        y_label: Y 轴标签

    Returns:
        Plotly 图表配置的 JSON 字符串
    """
    try:
        config = {
            "data": [{
                "type": chart_type,
                "x": x,
                "y": y,
            }],
            "layout": {
                "title": {"text": title},
                "xaxis": {"title": {"text": x_label}} if x_label else {},
                "yaxis": {"title": {"text": y_label}} if y_label else {},
            }
        }

        return json.dumps(config, ensure_ascii=False, indent=2)

    except Exception as e:
        return f"图表创建失败: {str(e)}"
```

### 5.3 工具注册

**文件**: `src/agent/tools/__init__.py`

```python
from typing import Callable

from src.agent.tools.snowflake_tool import snowflake_query
from src.agent.tools.chart_tool import create_chart


__all__ = [
    "snowflake_query",
    "create_chart",
    "get_default_tools",
]


def get_default_tools() -> list[Callable]:
    """获取默认工具列表"""
    return [
        snowflake_query,
        create_chart,
    ]
```

---

## 6. Agent 主入口

### 6.1 主入口实现

**文件**: `src/agent/agent.py`

```python
from typing import Optional

from deepagents import create_deep_agent

from src.core.config import Settings, get_settings
from src.agent.tools import get_default_tools
from src.agent.middleware.data_context import DataContextMiddleware
from src.agent.skills import get_skill_paths


SYSTEM_PROMPT = """You are an AI data analyst for AstraZeneca pharmaceutical data.

Your role is to help users analyze business data, answer questions about financial metrics,
market performance, and generate insights from the data warehouse.

## Core Capabilities

1. **SQL Analysis**: Generate and execute Snowflake queries
2. **Data Visualization**: Create charts and graphs from query results
3. **Report Generation**: Produce structured analysis reports

## Workflow

1. Understand the user's question completely
2. Generate appropriate SQL queries using the snowflake_query tool
3. Analyze the results and explain findings
4. Offer to create visualizations when helpful
5. Generate reports for complex analysis

## Guidelines

- Always use full table paths: ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>
- Use NULLIF to prevent division by zero
- Limit query results to reasonable sizes
- Explain your analysis in business terms
"""


def create_az_data_agent(
    settings: Optional[Settings] = None,
    verbose: bool = False,
):
    """
    创建 AZ 数据分析 Agent

    Args:
        settings: 应用配置
        verbose: 是否打印详细日志

    Returns:
        CompiledStateGraph (DeepAgent 实例)
    """
    settings = settings or get_settings()

    # 初始化工具
    tools = get_default_tools()

    # 初始化中间件
    middleware = [
        DataContextMiddleware(),
    ]

    # 获取技能目录路径
    skills = get_skill_paths()

    # 创建 DeepAgent
    # 注意: create_deep_agent 返回 CompiledStateGraph
    agent = create_deep_agent(
        model=settings.llm_model,
        tools=tools,
        middleware=middleware,
        skills=skills,
        system_prompt=SYSTEM_PROMPT,
    )

    return agent


def analyze_with_agent(
    question: str,
    settings: Optional[Settings] = None,
) -> str:
    """
    使用 Agent 分析问题

    Args:
        question: 用户问题
        settings: 应用配置

    Returns:
        Agent 分析结果
    """
    agent = create_az_data_agent(settings)
    result = agent.invoke({
        "messages": [{"role": "user", "content": question}]
    })
    # DeepAgent (CompiledStateGraph) 返回结果中提取 output
    if isinstance(result, dict):
        return result.get("output", "Unable to generate response")
    return str(result)
```

### 6.2 模块导出

**文件**: `src/agent/__init__.py`

```python
from src.agent.agent import create_az_data_agent, analyze_with_agent
from src.agent.middleware.data_context import DataContextMiddleware
from src.agent.middleware.alert_trigger import AlertTriggerHandler, get_alert_handler
from src.agent.tools import snowflake_query, create_chart, get_default_tools
from src.agent.skills import SKILLS_REGISTRY, get_skill_paths

__all__ = [
    "create_az_data_agent",
    "analyze_with_agent",
    "DataContextMiddleware",
    "AlertTriggerHandler",
    "get_alert_handler",
    "snowflake_query",
    "create_chart",
    "get_default_tools",
    "SKILLS_REGISTRY",
    "get_skill_paths",
]
```

---

## 7. 集成点

### 7.1 数据模型定义

**文件**: `src/monitor/models.py` (已有，补充 AlertStatus)

```python
from enum import Enum as PyEnum


class AlertStatus(str, PyEnum):
    """告警状态枚举"""
    PENDING = "pending"        # 等待处理
    PROCESSING = "processing"  # 正在处理
    COMPLETED = "completed"    # 已完成
    FAILED = "failed"          # 处理失败
```

### 7.2 初始化流程

应用启动时，需要按以下顺序初始化各组件：

```
┌─────────────────────────────────────────────────────────────────┐
│                      初始化流程                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Web 应用启动 (Streamlit)                                     │
│       │                                                          │
│       ▼                                                          │
│  2. create_az_data_agent()                                      │
│       │                                                          │
│       ├── 加载 Tools (snowflake_query, create_chart)            │
│       │                                                          │
│       ├── 加载 Middleware (DataContextMiddleware)               │
│       │                                                          │
│       └── 加载 Skills (sql_analyzer, data_visualizer, etc.)     │
│       │                                                          │
│       ▼                                                          │
│  3. get_alert_handler().set_agent_invoke(agent.invoke)          │
│       │                                                          │
│       │  ← 注册 Agent 回调，使告警处理器能调用 Agent             │
│       │                                                          │
│       ▼                                                          │
│  4. start_monitor_service()                                     │
│       │                                                          │
│       │  ← 启动监控调度器，定期执行指标检查                      │
│       │                                                          │
│       ▼                                                          │
│  5. 应用就绪，等待用户交互或告警触发                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.3 辅助函数定义

**文件**: `src/monitor/executor.py` (已有，补充说明)

```python
from src.core.database import execute_query
from src.core.config import Settings
from src.monitor.models import Metric


def execute_metric_sql(metric: Metric, settings: Settings) -> float | None:
    """
    执行指标的 SQL 模板并返回标量值

    Args:
        metric: 监控指标定义
        settings: 应用配置

    Returns:
        查询结果的标量值，如果查询失败返回 None
    """
    try:
        results = execute_query(metric.sql_template, settings)
        if results and results[0]:
            return float(results[0][0])
        return None
    except Exception as e:
        print(f"Error executing metric SQL: {e}")
        return None
```

**文件**: `src/monitor/alert_engine.py` (已有，补充说明)

```python
from src.monitor.models import ThresholdOperator


def check_threshold(
    actual_value: float | None,
    threshold_value: float,
    operator: ThresholdOperator,
) -> bool:
    """
    检查实际值是否触发告警

    Args:
        actual_value: 实际测量值
        threshold_value: 阈值
        operator: 比较运算符

    Returns:
        True 表示触发告警
    """
    if actual_value is None:
        return False

    comparisons = {
        ThresholdOperator.GT: actual_value > threshold_value,
        ThresholdOperator.LT: actual_value < threshold_value,
        ThresholdOperator.EQ: actual_value == threshold_value,
        ThresholdOperator.GTE: actual_value >= threshold_value,
        ThresholdOperator.LTE: actual_value <= threshold_value,
    }

    return comparisons.get(operator, False)
```

### 7.4 监控告警处理实现

**文件**: `src/monitor/alert_engine.py` (修改部分)

```python
from datetime import datetime
from sqlalchemy.orm import Session

from src.monitor.models import (
    AlertQueue,
    AlertStatus,
    Metric,
    MetricResult,
    get_session,  # 已存在于 models.py
)
from src.agent.middleware.alert_trigger import get_alert_handler


def process_metric(
    metric: Metric,
    settings: Settings,
    db_path: str = "data/monitor.db",
) -> MetricResult:
    """
    执行指标并处理告警

    Args:
        metric: 要执行的监控指标
        settings: 应用配置
        db_path: SQLite 数据库路径
    """
    # 执行 SQL 获取实际值
    actual_value = execute_metric_sql(metric, settings)

    # 阈值判断
    is_alert = check_threshold(
        actual_value,
        metric.threshold_value,
        metric.threshold_operator,
    )

    # 创建数据库会话 (get_session 已在 models.py 中定义)
    session: Session = get_session(db_path)

    try:
        # 创建执行结果记录
        result = MetricResult(
            metric_id=metric.id,
            actual_value=actual_value,
            threshold_value=metric.threshold_value,
            is_alert=is_alert,
        )
        session.add(result)
        session.commit()
        session.refresh(result)

        if is_alert:
            # 创建告警记录
            alert = AlertQueue(
                metric_id=metric.id,
                result_id=result.id,
                status=AlertStatus.PROCESSING,
            )
            session.add(alert)
            session.commit()
            session.refresh(alert)

            # 调用 Agent 进行分析
            alert_handler = get_alert_handler()
            analysis_result = alert_handler.on_alert(alert, metric, result)

            # 更新告警状态
            alert.status = AlertStatus.COMPLETED
            alert.analysis_result = analysis_result
            alert.processed_at = datetime.utcnow()
            session.commit()

    finally:
        session.close()

    return result
```

### 7.5 Web UI 集成

**文件**: `src/web/app.py` (修改部分)

```python
import streamlit as st

from src.agent.agent import create_az_data_agent
from src.agent.middleware.alert_trigger import get_alert_handler


def main():
    st.title("AZ Data Agent")

    # 初始化 Agent
    if "agent" not in st.session_state:
        agent = create_az_data_agent()
        st.session_state.agent = agent

        # 注册告警回调
        alert_handler = get_alert_handler()
        alert_handler.set_agent_invoke(agent.invoke)

    # ... 其他 UI 逻辑 ...
```

---

## 8. 错误处理策略

### 8.1 外部依赖错误处理

| 依赖 | 错误类型 | 处理策略 |
|------|---------|---------|
| Snowflake | 连接失败 | 返回友好错误信息，记录日志，不中断 Agent |
| Snowflake | 查询超时 | 设置查询超时（30秒），超时后返回提示用户简化查询 |
| Snowflake | SQL 语法错误 | 返回错误详情，提示用户检查查询 |
| LLM API | API 限流 | 重试机制（最多3次），指数退避 |
| LLM API | API 密钥无效 | 启动时验证，无效则拒绝启动 |
| Skills | 文件不存在 | 跳过缺失的 skill，记录警告日志 |

### 8.2 工具层错误处理

```python
# snowflake_tool.py 中的错误处理示例
def snowflake_query(sql: str) -> str:
    try:
        # ... 执行查询 ...
    except snowflake.connector.errors.DatabaseError as e:
        return f"数据库错误: {str(e)}"
    except snowflake.connector.errors.TimeoutError:
        return "查询超时，请尝试简化查询或减少数据量"
    except Exception as e:
        return f"查询执行失败: {str(e)}"
```

### 8.3 告警处理错误处理

```python
# alert_trigger.py 中的错误处理
def on_alert(self, alert, metric, result) -> str:
    try:
        response = self._agent_invoke({...})
        return response.get("output", "Analysis completed")
    except Exception as e:
        # 标记告警为失败状态
        return f"Analysis failed: {str(e)}"
```

---

## 9. 文件清单

| 文件 | 类型 | 说明 |
|------|------|------|
| `src/agent/agent.py` | 主入口 | DeepAgent 创建和调用 |
| `src/agent/__init__.py` | 模块导出 | 公开 API |
| `src/agent/middleware/data_context.py` | 中间件 | 数据上下文注入 (AgentMiddleware) |
| `src/agent/middleware/alert_trigger.py` | 处理器 | 告警触发回调 (AlertTriggerHandler) |
| `src/agent/context/business_context.py` | 配置 | 业务元数据 |
| `src/agent/tools/snowflake_tool.py` | 工具 | SQL 查询执行 |
| `src/agent/tools/chart_tool.py` | 工具 | 图表生成 |
| `src/agent/tools/__init__.py` | 模块导出 | 工具注册 |
| `src/agent/skills/__init__.py` | 模块导出 | Skills 注册 |
| `src/agent/skills/sql_analyzer/skill.md` | 技能 | SQL 分析指南 |
| `src/agent/skills/data_visualizer/skill.md` | 技能 | 可视化指南 |
| `src/agent/skills/report_generator/skill.md` | 技能 | 报告生成指南 |

---

## 10. 附录

### A. 依赖验证

**重要**: `deepagents` 是 LangChain 官方的 pre-1.0 包，可通过以下命令安装：

```bash
pip install deepagents
```

根据 LangChain 官方文档：
- `deepagents` 目前是 pre-1.0 版本（如 0.1.0）
- API 可能在 minor 版本间变化
- 版本 1.0 后将采用与 LangChain 相同的 LTS 策略

### B. 迁移注意事项

1. **依赖安装**: 需要 `pip install deepagents langchain>=0.3.0`
2. **API 变更**: `create_tool_calling_agent` → `create_deep_agent`
3. **返回格式**: DeepAgent (CompiledStateGraph) 返回格式可能与之前不同，需要适配
4. **测试覆盖**: 需要为新的 Middleware 和 Skills 编写测试

### C. 参考文档

- [LangChain DeepAgent 文档](https://docs.langchain.com/oss/python/deepagents/overview)
- [DeepAgent Middleware](https://docs.langchain.com/oss/python/langchain/middleware/built-in)
- [DeepAgent Skills](https://docs.langchain.com/oss/python/deepagents/skills)