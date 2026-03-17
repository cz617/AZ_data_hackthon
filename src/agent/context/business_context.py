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