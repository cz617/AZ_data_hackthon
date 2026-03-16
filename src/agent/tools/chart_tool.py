"""Chart tool for data visualization."""
from typing import Type
from pydantic import BaseModel, Field
from langchain.tools import BaseTool
import json


class ChartToolInput(BaseModel):
    """Input for Chart tool."""
    data: str = Field(description="JSON string containing data for visualization")
    chart_type: str = Field(description="Type of chart: 'bar', 'line', 'pie', 'scatter'")
    title: str = Field(default="", description="Chart title")


class ChartTool(BaseTool):
    """Tool for creating data visualizations using Plotly."""

    name: str = "create_chart"
    description: str = """
    Create data visualizations from query results.
    Supports bar charts, line charts, pie charts, and scatter plots.

    Input should be a JSON string with the data and chart configuration.
    """
    args_schema: Type[BaseModel] = ChartToolInput

    def _run(self, data: str, chart_type: str, title: str = "") -> str:
        """Generate Plotly chart configuration."""
        try:
            parsed_data = json.loads(data)

            chart_config = {
                "data": [{
                    "type": chart_type,
                    "x": parsed_data.get("x", []),
                    "y": parsed_data.get("y", []),
                    "labels": parsed_data.get("labels", []),
                    "values": parsed_data.get("values", []),
                }],
                "layout": {
                    "title": title or "Data Visualization",
                    "xaxis": {"title": parsed_data.get("x_label", "")},
                    "yaxis": {"title": parsed_data.get("y_label", "")},
                }
            }

            return json.dumps(chart_config, indent=2)
        except Exception as e:
            return f"Error creating chart: {str(e)}"

    async def _arun(self, data: str, chart_type: str, title: str = "") -> str:
        """Async execution."""
        return self._run(data, chart_type, title)