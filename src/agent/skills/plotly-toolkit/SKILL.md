---
name: plotly-toolkit
description: |
  数据可视化工具集。基于 Plotly 生成交互式图表。

  ## 功能
  - 智能图表类型推荐
  - 生成 Plotly 交互式 HTML 图表
  - 支持多种图表类型 (bar, line, scatter, pie, histogram, box, area)

  ## 输入格式
  - CSV 文件
  - JSON 数据
  - Excel 文件

  ## 使用流程
  1. 用 `db-toolkit` 执行查询，导出 CSV
  2. 用 `plotly-toolkit` 读取 CSV，生成图表

  ## 图表类型选择
  | 数据特征 | 推荐图表 |
  |----------|----------|
  | 时间序列 + 数值 | line |
  | 分类 + 数值 | bar |
  | 2 数值列 | scatter |
  | 单数值列分布 | histogram |
  | 占比分析 | pie |

allowed-tools: bash execute read_file write_file
---

# Plotly Toolkit

基于 Plotly Express 的数据可视化工具集。

## Scripts Reference

### `validate_data.py` - 数据验证

**在生成图表前务必验证数据**，防止运行时错误并确保图表兼容性。

```bash
# 验证数据是否适合指定图表类型
python scripts/validate_data.py --data results.csv --chart-type bar

# 指定列进行验证
python scripts/validate_data.py --data results.csv --chart-type bar \
    --x-column category --y-column revenue

# 自动推荐最佳图表类型
python scripts/validate_data.py --data results.csv --auto
```

#### 验证检查项

| 检查项 | 说明 | 错误级别 |
|-------|------|---------|
| 文件存在 | 数据文件可访问 | Error |
| 非空数据 | DataFrame 有行和列 | Error |
| 列存在 | 指定的列存在于数据中 | Error |
| 类型兼容 | 数据类型匹配图表要求 | Warning |
| 数据质量 | 空值、重复、常量列 | Suggestion |
| 图表适用性 | 图表类型适合数据特征 | Warning |

#### 验证输出示例

```
============================================================
Data File: results.csv
============================================================

📊 Data Summary:
  • Rows: 1250
  • Columns: 5
  • Numeric columns: ['revenue', 'quantity']
  • Categorical columns: ['product', 'region']
  • Chart type: bar

⚠️  WARNINGS:
  • Bar chart with two numeric columns - consider using scatter plot instead

💡 SUGGESTIONS:
  • Column 'region' has 45 null values (3.6%)

✅ Data is valid for chart generation
```

### `generate_chart.py` - 图表生成

从数据文件生成交互式 Plotly 图表。

```bash
# 自动推荐图表类型
python scripts/generate_chart.py --data results.csv --auto --output charts/revenue.html

# 指定图表类型
python scripts/generate_chart.py --data results.csv --chart-type bar \
    --x-column category --y-column revenue --output charts/revenue.html

# 时间序列折线图
python scripts/generate_chart.py --data monthly.csv --chart-type line \
    --x-column date --y-column sales --output charts/trend.html
```

## Chart Types

| Type | Use Case | Required Columns |
|------|----------|------------------|
| `bar` | 分类比较 | x (分类), y (数值) |
| `line` | 时间序列 | x (时间), y (数值) |
| `scatter` | 相关性分析 | x (数值), y (数值) |
| `pie` | 占比分析 | x (分类), y (数值) |
| `histogram` | 分布分析 | x (数值) |
| `box` | 分布比较 | y (数值), x (分类, 可选) |
| `area` | 累积趋势 | x (时间), y (数值) |

## Typical Workflow with db-toolkit

```bash
# 1. 用 db-toolkit 查询数据
python .amandax/skills/db-toolkit/scripts/execute.py \
    --database prod_dw \
    "SELECT date, SUM(revenue) as revenue FROM sales GROUP BY date" \
    --format csv --output data/daily_revenue.csv

# 2. 用 plotly-toolkit 生成图表
python .amandax/skills/plotly-toolkit/scripts/generate_chart.py \
    --data data/daily_revenue.csv \
    --chart-type line \
    --x-column date --y-column revenue \
    --output charts/daily_revenue.html
```

## Dependencies

```bash
pip install plotly pandas openpyxl
```
