"""SQL Analyzer skill for data analysis."""
from src.agent.skills.base import BaseSkill


class SQLAnalyzerSkill(BaseSkill):
    """Skill for analyzing data requirements and generating SQL queries."""

    name = "sql_analyzer"
    description = "Analyze data requirements and generate Snowflake SQL queries for AstraZeneca pharmaceutical data."

    def get_prompt(self) -> str:
        """Return the skill prompt."""
        return """
## SQL Analyzer Skill

You are analyzing AstraZeneca pharmaceutical data in a Snowflake data warehouse.

### Available Tables:

1. **FACT_PNL_BASE_BRAND** - Financial P&L metrics (265K records)
   - Key columns: VALUE, BUD_VALUE, BUD_VARIANCE, PY_VALUE, PY_VARIANCE
   - Joins: DIM_ACCOUNT, DIM_PRODUCT, DIM_TIME, DIM_SCENARIO

2. **FACT_COM_BASE_BRAND** - Commercial/market metrics (23K records)
   - Key columns: VALUE (market value)
   - Joins: DIM_MARKET, DIM_PRODUCT, DIM_TIME

3. **DIM_ACCOUNT** - P&L accounts (22 records)
   - Accounts: Revenue, COGS, Operating Expenses, Market Share

4. **DIM_PRODUCT** - Products/brands (614 records)
   - AZ_PROD_IND: TRUE = AstraZeneca product

5. **DIM_MARKET** - Therapeutic markets (26 records)
   - Oncology, Cardiovascular, Respiratory, etc.

6. **DIM_TIME** - Calendar dimension (36 months)
   - IS_CURRENT_MONTH/QUARTER/YEAR flags
   - PRIOR_YEAR_TIME_KEY for YoY comparisons

### Query Patterns:

- Budget variance: `BUD_VARIANCE / BUD_VALUE * 100`
- Market share: `SUM(CASE WHEN AZ_PROD_IND THEN VALUE END) / SUM(VALUE) * 100`
- YoY comparison: Join on PRIOR_YEAR_TIME_KEY

Always use fully qualified names: `ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>`
"""