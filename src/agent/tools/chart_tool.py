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