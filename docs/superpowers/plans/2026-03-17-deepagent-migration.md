# DeepAgent 架构迁移实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Agent 模块从 LangChain 0.3 `create_tool_calling_agent` 迁移到 DeepAgent `create_deep_agent`，添加 Skills 和 Middleware 支持。

**Architecture:** 采用分层架构：Tools（函数式）→ Middleware（AgentMiddleware）→ Skills（skill.md 文件）。DataContextMiddleware 注入业务上下文，AlertTriggerHandler 处理监控告警。

**Tech Stack:** Python 3.10+, LangChain 0.3+, deepagents (pre-1.0), Snowflake, Streamlit

---

## 文件结构

### 新建文件
| 文件 | 职责 |
|------|------|
| `src/agent/context/__init__.py` | 模块初始化 |
| `src/agent/context/business_context.py` | 业务元数据配置 |
| `src/agent/skills/__init__.py` | Skills 注册 |
| `src/agent/skills/sql_analyzer/skill.md` | SQL 分析技能 |
| `src/agent/skills/data_visualizer/skill.md` | 数据可视化技能 |
| `src/agent/skills/report_generator/skill.md` | 报告生成技能 |
| `tests/test_agent/test_deepagent.py` | DeepAgent 集成测试 |

### 修改文件
| 文件 | 变更说明 |
|------|---------|
| `pyproject.toml` | 添加 deepagents 依赖 |
| `src/agent/tools/snowflake_tool.py` | 从类改为函数式 |
| `src/agent/tools/chart_tool.py` | 从类改为函数式 |
| `src/agent/tools/__init__.py` | 导出函数而非类 |
| `src/agent/middleware/__init__.py` | 更新导出 |
| `src/agent/middleware/context_enricher.py` | 重写为 AgentMiddleware |
| `src/agent/middleware/alert_trigger.py` | 新建告警处理器 |
| `src/agent/agent.py` | 使用 create_deep_agent |
| `src/agent/__init__.py` | 更新模块导出 |
| `src/monitor/alert_engine.py` | 集成 AlertTriggerHandler |
| `src/web/app.py` | 更新 Agent 调用方式 |

---

## Chunk 1: 依赖和基础设施

### Task 1: 添加 deepagents 依赖

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: 添加 deepagents 依赖到 pyproject.toml**

```toml
dependencies = [
    "langchain>=0.3.0",
    "deepagents>=0.1.0",
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

- [ ] **Step 2: 验证依赖文件语法**

Run: `cat pyproject.toml | grep -A 20 "dependencies"`
Expected: 显示完整的依赖列表，包含 deepagents

- [ ] **Step 3: 提交依赖更新**

```bash
git add pyproject.toml
git commit -m "chore: Add deepagents dependency for DeepAgent migration"
```

---

### Task 2: 创建业务上下文配置模块

**Files:**
- Create: `src/agent/context/__init__.py`
- Create: `src/agent/context/business_context.py`

- [ ] **Step 1: 创建 context 模块初始化文件**

```python
# src/agent/context/__init__.py
"""Business context configuration for the data agent."""
from src.agent.context.business_context import BUSINESS_CONTEXT

__all__ = ["BUSINESS_CONTEXT"]
```

- [ ] **Step 2: 创建业务上下文配置**

```python
# src/agent/context/business_context.py
"""Business context configuration for Snowflake data warehouse."""

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

- [ ] **Step 3: 验证模块可以正常导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.context import BUSINESS_CONTEXT; print(len(BUSINESS_CONTEXT))"`
Expected: 输出一个正整数（字符串长度）

- [ ] **Step 4: 提交业务上下文模块**

```bash
git add src/agent/context/
git commit -m "feat(agent): Add business context configuration module"
```

---

### Task 3: 创建 Skills 模块结构

**Files:**
- Create: `src/agent/skills/__init__.py`
- Create: `src/agent/skills/sql_analyzer/skill.md`
- Create: `src/agent/skills/data_visualizer/skill.md`
- Create: `src/agent/skills/report_generator/skill.md`

- [ ] **Step 1: 创建 skills 模块初始化文件**

```python
# src/agent/skills/__init__.py
"""Skills module for the data agent."""
from pathlib import Path

SKILLS_DIR = Path(__file__).parent

SKILLS_REGISTRY = {
    "sql_analyzer": str(SKILLS_DIR / "sql_analyzer"),
    "data_visualizer": str(SKILLS_DIR / "data_visualizer"),
    "report_generator": str(SKILLS_DIR / "report_generator"),
}


def get_skill_paths() -> list[str]:
    """Get all skill directory paths for DeepAgent to load."""
    return list(SKILLS_REGISTRY.values())


__all__ = ["SKILLS_REGISTRY", "get_skill_paths", "SKILLS_DIR"]
```

- [ ] **Step 2: 创建 SQL 分析技能目录和文件**

Run: `mkdir -p /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/skills/sql_analyzer`

```markdown
# src/agent/skills/sql_analyzer/skill.md
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

- [ ] **Step 3: 创建数据可视化技能目录和文件**

Run: `mkdir -p /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/skills/data_visualizer`

```markdown
# src/agent/skills/data_visualizer/skill.md
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

- [ ] **Step 4: 创建报告生成技能目录和文件**

Run: `mkdir -p /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/skills/report_generator`

```markdown
# src/agent/skills/report_generator/skill.md
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

- [ ] **Step 5: 验证 skills 目录结构**

Run: `ls -la /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/skills/`
Expected: 显示 `__init__.py` 和三个技能目录

- [ ] **Step 6: 提交 Skills 模块**

```bash
git add src/agent/skills/
git commit -m "feat(agent): Add skills module with SQL analyzer, data visualizer, and report generator"
```

---

## Chunk 2: Tools 层重构

### Task 4: 重构 Snowflake 工具为函数式

**Files:**
- Modify: `src/agent/tools/snowflake_tool.py`

- [ ] **Step 1: 备份原文件**

Run: `cp /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/tools/snowflake_tool.py /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/tools/snowflake_tool.py.bak`

- [ ] **Step 2: 重写为函数式工具**

```python
# src/agent/tools/snowflake_tool.py
"""Snowflake tool for executing SQL queries."""
from langchain.tools import tool

from src.core.config import get_settings
from src.core.database import execute_query_with_columns


@tool
def snowflake_query(sql: str) -> str:
    """
    Execute SQL queries against the Snowflake data warehouse.
    Use this tool to query business data including P&L metrics, market data,
    product information, and time-based analysis.

    Available tables:
    - FACT_PNL_BASE_BRAND: Financial P&L metrics
    - FACT_COM_BASE_BRAND: Commercial/market metrics
    - DIM_ACCOUNT: P&L accounts
    - DIM_PRODUCT: Products/brands
    - DIM_MARKET: Therapeutic markets
    - DIM_TIME: Calendar dimension
    - DIM_SCENARIO: Planning scenarios

    Args:
        sql: SQL query to execute against Snowflake

    Returns:
        Query results formatted as readable text
    """
    settings = get_settings()
    try:
        columns, rows = execute_query_with_columns(sql, settings)
        if not rows:
            return "Query returned no results."

        # Format as readable text
        result = f"Columns: {', '.join(columns)}\n\n"
        result += f"Found {len(rows)} rows:\n"
        for i, row in enumerate(rows[:20]):  # Limit to 20 rows
            row_str = " | ".join(str(v) if v is not None else "NULL" for v in row)
            result += f"{i+1}. {row_str}\n"

        if len(rows) > 20:
            result += f"\n... and {len(rows) - 20} more rows"

        return result
    except Exception as e:
        return f"Error executing query: {str(e)}"
```

- [ ] **Step 3: 验证工具可以导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.tools.snowflake_tool import snowflake_query; print(snowflake_query.name)"`
Expected: 输出 `snowflake_query`

- [ ] **Step 4: 删除备份文件**

Run: `rm /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/tools/snowflake_tool.py.bak`

- [ ] **Step 5: 提交工具重构**

```bash
git add src/agent/tools/snowflake_tool.py
git commit -m "refactor(tools): Convert SnowflakeTool class to function-based tool"
```

---

### Task 5: 重构 Chart 工具为函数式

**Files:**
- Modify: `src/agent/tools/chart_tool.py`

- [ ] **Step 1: 备份原文件**

Run: `cp /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/tools/chart_tool.py /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/tools/chart_tool.py.bak`

- [ ] **Step 2: 重写为函数式工具**

```python
# src/agent/tools/chart_tool.py
"""Chart tool for data visualization."""
import json
from typing import Literal

from langchain.tools import tool


@tool
def create_chart(
    chart_type: Literal["bar", "line", "pie", "scatter"],
    x: list,
    y: list,
    title: str = "Data Visualization",
    x_label: str = "",
    y_label: str = "",
) -> str:
    """
    Create data visualizations from query results.
    Supports bar charts, line charts, pie charts, and scatter plots.

    Args:
        chart_type: Type of chart to create. Options: 'bar', 'line', 'pie', 'scatter'
        x: X-axis data (category labels or numeric values)
        y: Y-axis data (numeric values)
        title: Chart title
        x_label: Label for X-axis
        y_label: Label for Y-axis

    Returns:
        Plotly chart configuration as JSON string
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
        return f"Error creating chart: {str(e)}"
```

- [ ] **Step 3: 验证工具可以导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.tools.chart_tool import create_chart; print(create_chart.name)"`
Expected: 输出 `create_chart`

- [ ] **Step 4: 删除备份文件**

Run: `rm /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/tools/chart_tool.py.bak`

- [ ] **Step 5: 提交工具重构**

```bash
git add src/agent/tools/chart_tool.py
git commit -m "refactor(tools): Convert ChartTool class to function-based tool"
```

---

### Task 6: 更新工具模块导出

**Files:**
- Modify: `src/agent/tools/__init__.py`

- [ ] **Step 1: 更新工具模块初始化文件**

```python
# src/agent/tools/__init__.py
"""Agent tools package."""
from typing import Callable

from src.agent.tools.snowflake_tool import snowflake_query
from src.agent.tools.chart_tool import create_chart


__all__ = [
    "snowflake_query",
    "create_chart",
    "get_default_tools",
]


def get_default_tools() -> list[Callable]:
    """Get the list of default tools for the agent."""
    return [
        snowflake_query,
        create_chart,
    ]
```

- [ ] **Step 2: 验证模块导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.tools import get_default_tools; tools = get_default_tools(); print(len(tools))"`
Expected: 输出 `2`

- [ ] **Step 3: 提交工具模块更新**

```bash
git add src/agent/tools/__init__.py
git commit -m "refactor(tools): Update tools module to export function-based tools"
```

---

## Chunk 3: Middleware 层重构

### Task 7: 重构 DataContextMiddleware 为 AgentMiddleware

**Files:**
- Modify: `src/agent/middleware/context_enricher.py`

- [ ] **Step 1: 备份原文件**

Run: `cp /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/middleware/context_enricher.py /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/middleware/context_enricher.py.bak`

- [ ] **Step 2: 重命名为 DataContextMiddleware 并实现 AgentMiddleware**

```python
# src/agent/middleware/context_enricher.py
"""Data context middleware for injecting business context into agent."""
from typing import Callable

from langchain.agents.middleware import AgentMiddleware, ModelRequest, ModelResponse
from langchain.messages import SystemMessage

from src.agent.context.business_context import BUSINESS_CONTEXT


class DataContextMiddleware(AgentMiddleware):
    """
    Middleware that injects business context into the agent's system prompt.

    This middleware appends the business context (table structures, field descriptions,
    business metadata) to the system message content blocks before each LLM call.
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
        Inject context into the system message.

        Appends business context to the system_message content_blocks.
        """
        # Get current system message content blocks
        current_blocks = list(request.system_message.content_blocks)

        # Add business context block
        context_block = {
            "type": "text",
            "text": f"\n\n## 数据仓库上下文\n\n{self.context}"
        }
        new_blocks = current_blocks + [context_block]

        # Create new system message
        new_system_message = SystemMessage(content=new_blocks)

        # Call handler with modified request
        return handler(request.override(system_message=new_system_message))


# Keep backward compatibility alias
ContextEnricherMiddleware = DataContextMiddleware


__all__ = ["DataContextMiddleware", "ContextEnricherMiddleware"]
```

- [ ] **Step 3: 验证中间件可以导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.middleware.context_enricher import DataContextMiddleware; print(DataContextMiddleware.__bases__)"`
Expected: 输出包含 `AgentMiddleware`

- [ ] **Step 4: 删除备份文件**

Run: `rm /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/middleware/context_enricher.py.bak`

- [ ] **Step 5: 提交中间件重构**

```bash
git add src/agent/middleware/context_enricher.py
git commit -m "refactor(middleware): Convert ContextEnricherMiddleware to AgentMiddleware pattern"
```

---

### Task 8: 创建 AlertTriggerHandler

**Files:**
- Create: `src/agent/middleware/alert_trigger.py`

- [ ] **Step 1: 创建告警触发处理器**

```python
# src/agent/middleware/alert_trigger.py
"""Alert trigger handler for monitoring integration."""
from typing import Callable, Optional

from src.monitor.models import AlertQueue, Metric, MetricResult


class AlertTriggerHandler:
    """
    Handler for processing monitoring alerts.

    This is an event-driven handler (not an AgentMiddleware) that receives
    alerts from the monitoring module and triggers Agent analysis.

    Design Notes:
    - Does not implement wrap_model_call (not part of agent request flow)
    - Holds a reference to the agent's invoke method
    - Monitoring module calls on_alert() to trigger analysis
    """

    def __init__(self):
        self._agent_invoke: Optional[Callable] = None

    def set_agent_invoke(self, invoke_func: Callable):
        """
        Set the agent's invoke method.

        Must be called during web app initialization to register the agent callback.
        """
        self._agent_invoke = invoke_func

    def on_alert(
        self,
        alert: AlertQueue,
        metric: Metric,
        result: MetricResult,
    ) -> str:
        """
        Handle an alert event by triggering agent analysis.

        Args:
            alert: The alert queue record
            metric: The metric that triggered the alert
            result: The execution result with actual value

        Returns:
            Analysis result from the agent
        """
        if not self._agent_invoke:
            return "Agent not initialized"

        prompt = self._build_analysis_prompt(metric, result)

        try:
            response = self._agent_invoke({
                "messages": [{"role": "user", "content": prompt}]
            })
            # DeepAgent returns result in response["messages"][-1].content
            if response and "messages" in response and response["messages"]:
                return response["messages"][-1].content
            return "Analysis completed"
        except Exception as e:
            return f"Analysis failed: {str(e)}"

    def _build_analysis_prompt(
        self,
        metric: Metric,
        result: MetricResult,
    ) -> str:
        """Build the analysis prompt for the agent."""
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


# Global singleton
_alert_handler: Optional[AlertTriggerHandler] = None


def get_alert_handler() -> AlertTriggerHandler:
    """Get the alert handler singleton instance."""
    global _alert_handler
    if _alert_handler is None:
        _alert_handler = AlertTriggerHandler()
    return _alert_handler


__all__ = ["AlertTriggerHandler", "get_alert_handler"]
```

- [ ] **Step 2: 验证处理器可以导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.middleware.alert_trigger import get_alert_handler; print(type(get_alert_handler()).__name__)"`
Expected: 输出 `AlertTriggerHandler`

- [ ] **Step 3: 提交告警处理器**

```bash
git add src/agent/middleware/alert_trigger.py
git commit -m "feat(middleware): Add AlertTriggerHandler for monitoring integration"
```

---

### Task 9: 更新 Middleware 模块导出

**Files:**
- Modify: `src/agent/middleware/__init__.py`

- [ ] **Step 1: 更新中间件模块初始化文件**

```python
# src/agent/middleware/__init__.py
"""Agent middleware package."""
from src.agent.middleware.context_enricher import (
    DataContextMiddleware,
    ContextEnricherMiddleware,
)
from src.agent.middleware.alert_trigger import AlertTriggerHandler, get_alert_handler

__all__ = [
    "DataContextMiddleware",
    "ContextEnricherMiddleware",
    "AlertTriggerHandler",
    "get_alert_handler",
]
```

- [ ] **Step 2: 验证模块导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.middleware import DataContextMiddleware, AlertTriggerHandler, get_alert_handler; print('OK')"`
Expected: 输出 `OK`

- [ ] **Step 3: 提交中间件模块更新**

```bash
git add src/agent/middleware/__init__.py
git commit -m "refactor(middleware): Update middleware module exports"
```

---

## Chunk 4: Agent 主入口重构

### Task 10: 重构 Agent 主入口为 DeepAgent

**Files:**
- Modify: `src/agent/agent.py`

- [ ] **Step 1: 备份原文件**

Run: `cp /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/agent.py /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/agent.py.bak`

- [ ] **Step 2: 重写 Agent 主入口**

```python
# src/agent/agent.py
"""Main agent factory for creating the data analysis agent using DeepAgent."""
from typing import Optional

from deepagents import create_deep_agent

from src.core.config import Settings, get_settings
from src.agent.tools import get_default_tools
from src.agent.middleware import DataContextMiddleware
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
    Create the AZ data analysis agent using DeepAgent.

    Args:
        settings: Application settings (uses global if not provided)
        verbose: Whether to print agent reasoning (not used in DeepAgent)

    Returns:
        CompiledStateGraph (DeepAgent instance)
    """
    settings = settings or get_settings()

    # Initialize tools (function-based)
    tools = get_default_tools()

    # Initialize middleware
    middleware = [
        DataContextMiddleware(),
    ]

    # Get skill directory paths
    skills = get_skill_paths()

    # Create DeepAgent
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
    Analyze a question using the data agent.

    Args:
        question: User's question about the data
        settings: Application settings

    Returns:
        Agent's response
    """
    agent = create_az_data_agent(settings)
    result = agent.invoke({
        "messages": [{"role": "user", "content": question}]
    })
    # DeepAgent returns result in result["messages"][-1].content
    if result and "messages" in result and result["messages"]:
        return result["messages"][-1].content
    return "Unable to generate response"


# Backward compatibility alias
create_data_agent = create_az_data_agent


__all__ = [
    "create_az_data_agent",
    "create_data_agent",
    "analyze_with_agent",
]
```

- [ ] **Step 3: 验证 Agent 可以创建（需要 deepagents 安装）**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent.agent import create_az_data_agent, analyze_with_agent; print('Import OK')" 2>&1 || echo "Note: deepagents package required"`
Expected: 如果 deepagents 已安装则输出 `Import OK`，否则提示需要安装

- [ ] **Step 4: 删除备份文件**

Run: `rm /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/agent/agent.py.bak`

- [ ] **Step 5: 提交 Agent 重构**

```bash
git add src/agent/agent.py
git commit -m "feat(agent): Migrate from create_tool_calling_agent to create_deep_agent"
```

---

### Task 11: 更新 Agent 模块导出

**Files:**
- Modify: `src/agent/__init__.py`

- [ ] **Step 1: 更新 Agent 模块初始化文件**

```python
# src/agent/__init__.py
"""Data Agent module for AZ Data Agent."""
from src.agent.agent import (
    create_az_data_agent,
    create_data_agent,
    analyze_with_agent,
)
from src.agent.middleware import (
    DataContextMiddleware,
    ContextEnricherMiddleware,
    AlertTriggerHandler,
    get_alert_handler,
)
from src.agent.tools import (
    snowflake_query,
    create_chart,
    get_default_tools,
)
from src.agent.skills import (
    SKILLS_REGISTRY,
    get_skill_paths,
    SKILLS_DIR,
)

__all__ = [
    # Agent factory
    "create_az_data_agent",
    "create_data_agent",
    "analyze_with_agent",
    # Middleware
    "DataContextMiddleware",
    "ContextEnricherMiddleware",
    "AlertTriggerHandler",
    "get_alert_handler",
    # Tools
    "snowflake_query",
    "create_chart",
    "get_default_tools",
    # Skills
    "SKILLS_REGISTRY",
    "get_skill_paths",
    "SKILLS_DIR",
]
```

- [ ] **Step 2: 验证模块导入**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.agent import create_az_data_agent, get_alert_handler; print('OK')"`
Expected: 输出 `OK`

- [ ] **Step 3: 提交模块导出更新**

```bash
git add src/agent/__init__.py
git commit -m "refactor(agent): Update agent module exports for DeepAgent"
```

---

## Chunk 5: 集成更新

### Task 12: 更新 Web UI 集成

**Files:**
- Modify: `src/web/app.py`

- [ ] **Step 1: 备份原文件**

Run: `cp /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/web/app.py /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/web/app.py.bak`

- [ ] **Step 2: 更新 Web UI 使用新的 Agent API**

```python
# src/web/app.py
"""Main Streamlit application."""
import streamlit as st
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from src.core.config import get_settings
from src.agent.agent import create_az_data_agent
from src.agent.middleware import get_alert_handler

# Page config
st.set_page_config(
    page_title="AZ Data Agent",
    page_icon="📊",
    layout="wide",
)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    try:
        settings = get_settings()
        agent = create_az_data_agent(settings)
        st.session_state.agent = agent

        # Register alert handler callback
        alert_handler = get_alert_handler()
        alert_handler.set_agent_invoke(agent.invoke)
    except Exception as e:
        st.error(f"Failed to initialize agent: {e}")
        st.session_state.agent = None


def main():
    """Main application."""
    st.title("📊 AZ Data Agent")
    st.markdown("AI-powered data analysis for AstraZeneca pharmaceutical data")

    # Sidebar
    with st.sidebar:
        st.header("About")
        st.markdown("""
        This tool helps you analyze:
        - **P&L Metrics**: Revenue, costs, budget variance
        - **Market Share**: AZ vs competitor performance
        - **Trends**: Year-over-year comparisons

        Just ask a question in natural language!
        """)

        st.divider()

        st.header("Example Questions")
        st.markdown("""
        - What is our current quarter revenue?
        - Show budget variance by product
        - What is AZ's market share in Oncology?
        - Compare revenue YoY by therapeutic area
        """)

        if st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()

    # Main chat area
    chat_container = st.container()

    with chat_container:
        # Display chat history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask about your data..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                try:
                    if st.session_state.agent:
                        # DeepAgent invoke format
                        result = st.session_state.agent.invoke({
                            "messages": [{"role": "user", "content": prompt}]
                        })
                        # Extract response from messages
                        if result and "messages" in result and result["messages"]:
                            response = result["messages"][-1].content
                        else:
                            response = "Unable to generate response"
                    else:
                        response = "Agent not initialized. Please check configuration."

                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: 删除备份文件**

Run: `rm /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/web/app.py.bak`

- [ ] **Step 4: 提交 Web UI 更新**

```bash
git add src/web/app.py
git commit -m "feat(web): Update app.py to use DeepAgent and AlertTriggerHandler"
```

---

### Task 13: 更新监控模块集成

**Files:**
- Modify: `src/monitor/alert_engine.py`

- [ ] **Step 1: 备份原文件**

Run: `cp /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/monitor/alert_engine.py /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/monitor/alert_engine.py.bak`

- [ ] **Step 2: 更新 process_metric 函数集成 AlertTriggerHandler**

在现有代码基础上，修改 `process_metric` 函数：

```python
# src/monitor/alert_engine.py
# 在文件开头添加导入
from src.agent.middleware.alert_trigger import get_alert_handler


def process_metric(
    metric: Metric,
    settings: Settings,
    db_path: str = "data/monitor.db",
) -> MetricResult:
    """
    Execute a metric and create result record.
    If alert is triggered, call Agent for automatic analysis.

    Args:
        metric: Metric to process
        settings: Application settings
        db_path: Path to SQLite database

    Returns:
        Created MetricResult
    """
    actual_value = execute_metric_sql(metric, settings)

    is_alert = check_threshold(
        actual_value,
        metric.threshold_value,
        metric.threshold_operator,
    )

    session = get_session(db_path)
    try:
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
            # Create alert record with PROCESSING status
            alert = AlertQueue(
                metric_id=metric.id,
                result_id=result.id,
                status=AlertStatus.PROCESSING,
            )
            session.add(alert)
            session.commit()
            session.refresh(alert)

            # Call Agent for analysis
            alert_handler = get_alert_handler()
            try:
                analysis_result = alert_handler.on_alert(alert, metric, result)
                # Update alert status to COMPLETED
                alert.status = AlertStatus.COMPLETED
                alert.analysis_result = analysis_result
            except Exception as e:
                # Update alert status to FAILED
                alert.status = AlertStatus.FAILED
                alert.analysis_result = f"Analysis failed: {str(e)}"
            finally:
                alert.processed_at = datetime.utcnow()
                session.commit()

    finally:
        session.close()

    return result
```

- [ ] **Step 3: 验证导入正确**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -c "from src.monitor.alert_engine import process_metric; print('OK')"`
Expected: 输出 `OK`

- [ ] **Step 4: 删除备份文件**

Run: `rm /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/src/monitor/alert_engine.py.bak`

- [ ] **Step 5: 提交监控模块更新**

```bash
git add src/monitor/alert_engine.py
git commit -m "feat(monitor): Integrate AlertTriggerHandler for automatic alert analysis"
```

---

## Chunk 6: 测试和验证

### Task 14: 创建 DeepAgent 集成测试

**Files:**
- Create: `tests/test_agent/test_deepagent.py`

- [ ] **Step 1: 创建测试目录（如不存在）**

Run: `mkdir -p /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon/tests/test_agent`

- [ ] **Step 2: 创建测试文件**

```python
# tests/test_agent/test_deepagent.py
"""Tests for DeepAgent integration."""
import pytest


class TestTools:
    """Tests for function-based tools."""

    def test_snowflake_query_is_callable(self):
        """Test that snowflake_query is a callable tool."""
        from src.agent.tools import snowflake_query
        assert callable(snowflake_query)
        assert hasattr(snowflake_query, 'name')
        assert snowflake_query.name == "snowflake_query"

    def test_create_chart_is_callable(self):
        """Test that create_chart is a callable tool."""
        from src.agent.tools import create_chart
        assert callable(create_chart)
        assert hasattr(create_chart, 'name')
        assert create_chart.name == "create_chart"

    def test_get_default_tools_returns_list(self):
        """Test that get_default_tools returns a list of tools."""
        from src.agent.tools import get_default_tools
        tools = get_default_tools()
        assert isinstance(tools, list)
        assert len(tools) == 2


class TestMiddleware:
    """Tests for middleware components."""

    def test_data_context_middleware_is_agent_middleware(self):
        """Test that DataContextMiddleware inherits from AgentMiddleware."""
        from langchain.agents.middleware import AgentMiddleware
        from src.agent.middleware import DataContextMiddleware
        assert issubclass(DataContextMiddleware, AgentMiddleware)

    def test_alert_trigger_handler_has_required_methods(self):
        """Test that AlertTriggerHandler has required methods."""
        from src.agent.middleware import AlertTriggerHandler
        handler = AlertTriggerHandler()
        assert hasattr(handler, 'set_agent_invoke')
        assert hasattr(handler, 'on_alert')
        assert callable(handler.set_agent_invoke)
        assert callable(handler.on_alert)

    def test_get_alert_handler_returns_singleton(self):
        """Test that get_alert_handler returns the same instance."""
        from src.agent.middleware import get_alert_handler
        handler1 = get_alert_handler()
        handler2 = get_alert_handler()
        assert handler1 is handler2


class TestSkills:
    """Tests for skills module."""

    def test_skills_registry_has_required_skills(self):
        """Test that skills registry contains all required skills."""
        from src.agent.skills import SKILLS_REGISTRY
        assert "sql_analyzer" in SKILLS_REGISTRY
        assert "data_visualizer" in SKILLS_REGISTRY
        assert "report_generator" in SKILLS_REGISTRY

    def test_get_skill_paths_returns_list(self):
        """Test that get_skill_paths returns a list of strings."""
        from src.agent.skills import get_skill_paths
        paths = get_skill_paths()
        assert isinstance(paths, list)
        assert len(paths) == 3
        for path in paths:
            assert isinstance(path, str)

    def test_skill_files_exist(self):
        """Test that all skill.md files exist."""
        from src.agent.skills import SKILLS_DIR
        import os
        for skill_name in ["sql_analyzer", "data_visualizer", "report_generator"]:
            skill_path = SKILLS_DIR / skill_name / "skill.md"
            assert os.path.exists(skill_path), f"skill.md not found for {skill_name}"


class TestBusinessContext:
    """Tests for business context configuration."""

    def test_business_context_is_string(self):
        """Test that BUSINESS_CONTEXT is a non-empty string."""
        from src.agent.context import BUSINESS_CONTEXT
        assert isinstance(BUSINESS_CONTEXT, str)
        assert len(BUSINESS_CONTEXT) > 0

    def test_business_context_contains_required_sections(self):
        """Test that BUSINESS_CONTEXT contains required sections."""
        from src.agent.context import BUSINESS_CONTEXT
        assert "FACT_PNL_BASE_BRAND" in BUSINESS_CONTEXT
        assert "FACT_COM_BASE_BRAND" in BUSINESS_CONTEXT
        assert "DIM_PRODUCT" in BUSINESS_CONTEXT
        assert "DIM_TIME" in BUSINESS_CONTEXT
```

- [ ] **Step 3: 运行测试**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -m pytest tests/test_agent/test_deepagent.py -v`
Expected: 所有测试通过

- [ ] **Step 4: 提交测试文件**

```bash
git add tests/test_agent/test_deepagent.py
git commit -m "test(agent): Add integration tests for DeepAgent migration"
```

---

### Task 15: 运行完整测试套件

**Files:**
- None (validation only)

- [ ] **Step 1: 运行所有测试**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -m pytest tests/ -v`
Expected: 所有测试通过

- [ ] **Step 2: 检查代码风格**

Run: `cd /Users/jasonzchen/Documents/GitHub/AZ_data_hackthon && python -m black src/agent --check && python -m isort src/agent --check`
Expected: 无格式错误

- [ ] **Step 3: 最终提交（如有遗漏）**

```bash
git status
# 如果有未提交的更改，提交它们
git add -A
git commit -m "chore: Final cleanup for DeepAgent migration"
```

---

## 验收标准

- [ ] `deepagents` 依赖已添加到 `pyproject.toml`
- [ ] 工具已从类式重构为函数式（`@tool` 装饰器）
- [ ] `DataContextMiddleware` 继承自 `AgentMiddleware`
- [ ] `AlertTriggerHandler` 可处理监控告警
- [ ] Skills 目录结构正确，`skill.md` 文件存在
- [ ] Agent 使用 `create_deep_agent` 创建
- [ ] Web UI 正确调用 DeepAgent（`result["messages"][-1].content`）
- [ ] 监控模块集成 `AlertTriggerHandler`
- [ ] 所有测试通过

---

## 执行顺序

1. Chunk 1: 依赖和基础设施（Task 1-3）
2. Chunk 2: Tools 层重构（Task 4-6）
3. Chunk 3: Middleware 层重构（Task 7-9）
4. Chunk 4: Agent 主入口重构（Task 10-11）
5. Chunk 5: 集成更新（Task 12-13）
6. Chunk 6: 测试和验证（Task 14-15）