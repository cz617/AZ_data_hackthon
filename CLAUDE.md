# AZ Data Agent 项目规范

## 项目概述

AI Agent 数据分析系统，基于 AstraZeneca 医药数据构建，支持智能监控和自动分析。

### 核心功能
- **智能监控**: 定时执行 SQL 监控指标，自动检测异常
- **AI 分析**: LangChain Agent 自动分析数据，回答用户问题
- **自动告警**: 异常触发时自动调用 Agent 进行深度分析

## 技术栈

| 组件 | 技术 |
|------|------|
| LLM 框架 | LangChain + Middleware + Skills |
| LLM 模型 | 可配置（Claude/OpenAI/Azure） |
| 数据库 | Snowflake |
| 任务队列 | SQLite |
| Web UI | Streamlit |
| 定时任务 | APScheduler |

## 项目结构

```
az-data-agent/
├── src/
│   ├── core/          # 核心共享模块（配置、数据库、LLM）
│   ├── monitor/       # 监控服务（调度、执行、告警）
│   ├── agent/         # Data Agent（中间件、Skills、Tools）
│   ├── web/           # Web UI（Streamlit）
│   └── messaging/     # 消息队列（SQLite）
├── config/            # 配置文件
├── tests/             # 测试
└── docs/              # 文档
```

## 开发规范

### Python 版本
- Python 3.10+

### 代码风格
- 使用 Black 格式化
- 使用 isort 排序导入
- 类型注解必须

### 配置管理
- 使用 Pydantic Settings
- 敏感信息通过环境变量传递
- 配置文件位于 `config/settings.yaml`

### 数据库连接
- Snowflake 连接使用连接池
- 查询使用参数化，防止注入
- 完整路径: `ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>`

### Agent 开发
- 新增能力通过 Skills 扩展
- Middleware 用于注入上下文和加载 Skills
- Tools 是 Agent 可调用的原子操作

## 常用命令

```bash
# 安装依赖
pip install -r requirements.txt

# 启动 Web UI
streamlit run src/web/app.py

# 启动监控服务
python -m src.monitor.scheduler

# 运行测试
pytest tests/

# 格式化代码
black src/ && isort src/
```

## 环境变量

```bash
# LLM 配置
LLM_PROVIDER=claude          # claude | openai | azure
LLM_API_KEY=xxx

# Snowflake 配置
SNOWFLAKE_ACCOUNT=xxx
SNOWFLAKE_USER=xxx
SNOWFLAKE_PASSWORD=xxx
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

## 参考文档

- 设计文档: `docs/superpowers/specs/2026-03-16-az-data-agent-design.md`
- 数据模型: `000_客户提供的资料/HACKATHON_Data_Model_v2.md`
- 数据字典: `000_客户提供的资料/HACKATHON_Data_Dictionary_v2.md`

## 注意事项

1. **无状态 Agent**: 每次对话独立，不保留历史记忆
2. **配置文件管理**: SQL 模板通过 YAML 配置，不提供 UI 编辑
3. **SQLite 队列**: 不使用外部消息队列，用 SQLite 实现简单队列
4. **单一 Web 服务**: Web UI 和监控在同一进程运行