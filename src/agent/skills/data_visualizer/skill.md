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