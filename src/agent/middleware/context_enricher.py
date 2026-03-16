"""Context enricher middleware for data model injection."""
from typing import Any, Dict


class ContextEnricherMiddleware:
    """Middleware to inject business context into agent."""

    DATA_CONTEXT = """
## AstraZeneca Data Context

### Business Domain
Pharmaceutical company data for Spain and Brazil markets.

### Data Model
- Star schema with dual fact tables (P&L and Commercial)
- 36 months of historical data (2023-2025)
- Multi-scenario planning (Actual, Budget, MTP, LTP)

### Key Metrics
- Revenue and profitability analysis
- Market share tracking
- Budget variance analysis
- Year-over-year comparisons

### Geographic Scope
- Spain (Management Unit: 44000ES)
- Brazil (Management Unit: 44000BR)

### Product Categories
- Oncology TA
- BioPharma TA (CVRM)
- Rare Disease TA
- Central TA
"""

    def enrich_prompt(self, prompt: str) -> str:
        """Add data context to the prompt."""
        return f"{prompt}\n\n{self.DATA_CONTEXT}"