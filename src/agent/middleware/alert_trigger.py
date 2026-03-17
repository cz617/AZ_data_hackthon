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