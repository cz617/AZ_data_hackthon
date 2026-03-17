# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

阿斯利康医药数据的 AI 智能分析系统。支持定时 SQL 监控指标执行、LangChain Agent 数据分析、以及阈值突破时的自动告警。

## 架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AZ Data Agent System                         │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐        │
│  │   Web UI     │     │   Monitor    │     │  Data Agent  │        │
│  │  (Streamlit) │     │   Service    │     │ (DeepAgent)  │        │
│  │  Port: 8501  │     │  Background  │     │  On-Demand   │        │
│  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘        │
│         └────────────────────┴────────────────────┘                 │
│                              │                                       │
│                   ┌──────────┴──────────┐                           │
│                   │   Shared Services   │                           │
│                   │  - Config (Pydantic)│                           │
│                   │  - LLM Provider     │                           │
│                   │  - Snowflake Client │                           │
│                   │  - SQLite Queue     │                           │
│                   └─────────────────────┘                           │
└─────────────────────────────────────────────────────────────────────┘
```

### 核心组件

- **Agent**: 使用 `deepagents.create_deep_agent()` 构建，集成 LangChain 工具和中间件
- **Tools**: LangChain `SQLDatabaseToolkit` 提供 4 个工具：`sql_db_query`、`sql_db_schema`、`sql_db_list_tables`、`sql_db_query_checker`
- **Middleware**: `DataContextMiddleware` 向 Agent 会话注入业务上下文
- **Skills**: 通过 `get_skill_paths()` 从 `src/agent/skills/` 目录加载

### 数据流

**用户查询**: Web UI → DeepAgent → Snowflake Tools → 结果返回
**监控告警**: APScheduler → 指标执行器 → 告警引擎 → Agent 分析

## 关键文件

| 路径 | 用途 |
|------|------|
| `src/core/config.py` | Pydantic Settings 配置管理，支持环境变量加载 |
| `src/agent/agent.py` | DeepAgent 工厂函数，配置工具和中间件 |
| `src/agent/tools/snowflake.py` | Snowflake 连接池和 SQLDatabaseToolkit |
| `src/monitor/scheduler.py` | 基于 APScheduler 的指标监控调度 |
| `src/detect/api.py` | FastAPI 差异检测接口 |

## 常用命令

```bash
# 安装依赖
pip install -r requirements.txt

# 启动 Web UI (端口 8501)
streamlit run src/web/app.py

# 启动监控服务
python -m src.monitor.scheduler

# 初始化 SQLite 数据库
python scripts/init_db.py

# 运行所有测试
pytest tests/

# 运行单个测试文件
pytest tests/test_agent/test_deepagent.py -v

# 启动差异检测 API
python -m src.detect.api
```

## 配置

环境变量（通过 `.env` 文件配置）：

```bash
# LLM 配置
LLM_PROVIDER=claude          # claude | openai | azure
LLM_MODEL=claude-sonnet-4-5-20250929
LLM_API_KEY=xxx

# Snowflake 配置
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USER=xxx
SNOWFLAKE_PASSWORD=xxx
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

主配置文件：`config/settings.yaml`（支持 `${VAR:default}` 语法）

## 数据库路径

- Snowflake: `ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>`
- SQLite (监控): `data/monitor.db`

## 参考文档

- 设计文档: `docs/superpowers/specs/2026-03-16-az-data-agent-design.md`
- 数据模型: `000_客户提供的资料/HACKATHON_Data_Model_v2.md`
- 数据字典: `000_客户提供的资料/HACKATHON_Data_Dictionary_v2.md`