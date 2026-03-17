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