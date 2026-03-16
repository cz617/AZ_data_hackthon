# EA_HACKATHON Data Model (Updated)

## Overview
This schema implements a **dual-fact star schema** design for comprehensive financial and commercial analysis. It supports both Profit & Loss (P&L) and Commercial/Market performance tracking with multi-scenario planning capabilities and hierarchical dimensional analysis.

---

## Schema Information
- **Database:** ENT_HACKATHON_DATA_SHARE
- **Schema:** EA_HACKATHON
- **Model Type:** Star Schema with Hierarchical Dimensions
- **Total Tables:** 11 (2 Facts + 5 Dimensions + 4 Hierarchies)
- **Total Records:** ~289,000 fact records
- **Data Period:** January 2023 - December 2025 (3 years, 36 months)

---

## Data Model Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DIMENSION LAYER                                  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────┐        ┌──────────────────────────┐               │
│  │  DIM_SCENARIO   │        │      DIM_TIME            │               │
│  │  (4 records)    │        │     (36 records)         │               │
│  │─────────────────│        │──────────────────────────│               │
│  │ SCENARIO_KEY    │        │ TIME_KEY (PK)            │               │
│  │ SCENARIO_NAME   │        │ DATE_KEY, MONTH, QUARTER │               │
│  └────────┬────────┘        │ YEAR, YTD_MONTH_NUMBER   │               │
│           │                 │ PRIOR_YEAR_TIME_KEY      │               │
│           │                 │ IS_CURRENT_* flags       │               │
│           │                 └──────────┬───────────────┘               │
│           │                            │                               │
│  ┌────────┴────────────────────────────┴─────────────────┐             │
│  │         FACT_PNL_BASE_BRAND (P&L Focus)               │             │
│  │              265,104 records                          │             │
│  ├───────────────────────────────────────────────────────┤             │
│  │ Keys: ACCOUNT_KEY, MANAGEMENT_UNIT_KEY, PRODUCT_KEY,    │             │
│  │       SCENARIO_KEY, TIME_KEY                          │             │
│  │ Measures: VALUE, QTD, YTD, Q1-Q4, H1-H2              │             │
│  │ Scenarios: PY_*, BUD_*, MTP_*, RBU2LTP_*             │             │
│  │ Variances: 9 variance columns                         │             │
│  └──────┬──────────────────┬──────────────┬─────────────┘             │
│         │                  │              │                            │
│  ┌──────▼──────────┐  ┌────▼──────────┐ ┌▼────────────────┐          │
│  │  DIM_ACCOUNT    │  │ DIM_MGMT_UNIT │ │  DIM_PRODUCT    │          │
│  │  (22 records)   │  │  (2 records)  │ │  (614 records)  │          │
│  │─────────────────│  │───────────────│ │─────────────────│          │
│  │ ACCOUNT_KEY     │  │ MU_KEY        │ │ PRODUCT_KEY       │          │
│  │ ACCOUNT_CODE    │  │ MU_CODE       │ │ = PRODUCT_KEY   │          │
│  │ ACCOUNT_DESC    │  │ MU_NAME       │ │ PRODUCT_CODE    │          │
│  └────────┬────────┘  └───────┬───────┘ │ PRODUCT_DESC    │          │
│           │                   │         │ AZ_PROD_IND     │          │
│           │                   │         └────────┬────────┘          │
│  ┌────────▼──────────────┐ ┌──▼────────────────────────┐             │
│  │ DIM_ACCOUNT_HIERARCHY │ │ DIM_MU_HIERARCHY          │             │
│  │    (23 records)       │ │    (2 records)            │             │
│  ├───────────────────────┤ ├───────────────────────────┤             │
│  │ 12-Level Hierarchy    │ │ 10-Level Hierarchy        │             │
│  │ LEVEL1-12 CODE/DESC   │ │ LEVEL1-10 CODE/DESC       │             │
│  │ ACCOUNT_KEY           │ │ MANAGEMENT_UNIT_KEY       │             │
│  └───────────────────────┘ └───────────────────────────┘             │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────┐          │
│  │      FACT_COM_BASE_BRAND (Commercial Focus)             │          │
│  │              23,568 records                             │          │
│  ├─────────────────────────────────────────────────────────┤          │
│  │ Keys: ACCOUNT_KEY, MARKET_KEY, MANAGEMENT_UNIT_KEY,     │          │
│  │       PRODUCT_KEY, SCENARIO_KEY, TIME_KEY               │          │
│  │ Measures: VALUE (single metric)                         │          │
│  └──┬──────────────────┬─────────────────────┬────────────┘          │
│     │                  │                     │                        │
│  ┌──▼───────────┐  ┌───▼─────────────┐  ┌───▼───────────────┐       │
│  │  DIM_MARKET  │  │   DIM_PRODUCT   │  │ DIM_PRODUCT_HIER. │       │
│  │ (26 records) │  │  (614 records)  │  │  (174 records)    │       │
│  │──────────────│  │─────────────────│  │───────────────────│       │
│  │ MARKET_KEY   │  │ PRODUCT_KEY     │  │ 10-Level Hier.    │       │
│  │ MARKET_NAME  │  │ PRODUCT_CODE    │  │ LEVEL1-10 CODE    │       │
│  │ SUB_MARKET   │  │ PRODUCT_DESC    │  │ PRODUCT_KEY       │       │
│  └──────────────┘  │ AZ_PROD_IND     │  └───────────────────┘       │
│                    └─────────────────┘                                │
│                                                                        │
│  NOTE: DIM_PRODUCT and DIM_PRODUCT share the same keys and data        │
│        (PRODUCT_KEY = PRODUCT_KEY, used interchangeably)                │
│                                                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Table Inventory

### Fact Tables (2)

| Table Name | Records | Size | Grain | Purpose |
|------------|---------|------|-------|---------|
| **FACT_PNL_BASE_BRAND** | 265,104 | 19.1 MB | Account-MU-Brand-Scenario-Time | P&L financial metrics with multi-scenario variance analysis |
| **FACT_COM_BASE_BRAND** | 23,568 | 142 KB | Account-Market-MU-Product-Scenario-Time | Commercial/market performance metrics |

### Dimension Tables (5)

| Table Name | Records | Purpose |
|------------|---------|---------|
| **DIM_ACCOUNT** | 22 | Chart of accounts (P&L line items) |
| **DIM_MANAGEMENT_UNIT** | 2 | Organizational units (Spain & Brazil) |
| **DIM_PRODUCT** | 614 | Product/Brand master |
| **DIM_SCENARIO** | 4 | Planning scenarios (Actual, Budget, Forecast, etc.) |
| **DIM_TIME** | 36 | Calendar dimension with time intelligence |
| **DIM_MARKET** | 26 | Market/therapeutic categories |

### Hierarchy Tables (4)

| Table Name | Records | Levels | Purpose |
|------------|---------|--------|---------|
| **DIM_ACCOUNT_HIERARCHY** | 23 | 12 | Account rollup structure for P&L reporting |
| **DIM_MANAGEMENT_UNIT_HIERARCHY** | 2 | 10 | Organizational rollup hierarchy |
| **DIM_PRODUCT_HIERARCHY** | 174 | 10 | Product/Brand taxonomy (TA, therapy areas) |

---

## Key Relationships

### FACT_PNL_BASE_BRAND Relationships
```sql
FACT_PNL_BASE_BRAND.ACCOUNT_KEY          → DIM_ACCOUNT.ACCOUNT_KEY
FACT_PNL_BASE_BRAND.MANAGEMENT_UNIT_KEY → DIM_MANAGEMENT_UNIT.MANAGEMENT_UNIT_KEY
FACT_PNL_BASE_BRAND.PRODUCT_KEY         → DIM_PRODUCT.PRODUCT_KEY
FACT_PNL_BASE_BRAND.SCENARIO_KEY        → DIM_SCENARIO.SCENARIO_KEY
FACT_PNL_BASE_BRAND.TIME_KEY            → DIM_TIME.TIME_KEY

-- Hierarchies
DIM_ACCOUNT.ACCOUNT_KEY                  → DIM_ACCOUNT_HIERARCHY.ACCOUNT_KEY
DIM_MANAGEMENT_UNIT.MANAGEMENT_UNIT_KEY  → DIM_MANAGEMENT_UNIT_HIERARCHY.MANAGEMENT_UNIT_KEY
DIM_PRODUCT.PRODUCT_KEY                  → DIM_PRODUCT_HIERARCHY.PRODUCT_KEY
```

### FACT_COM_BASE_BRAND Relationships
```sql
FACT_COM_BASE_BRAND.ACCOUNT_KEY          → DIM_ACCOUNT.ACCOUNT_KEY
FACT_COM_BASE_BRAND.MARKET_KEY           → DIM_MARKET.MARKET_KEY
FACT_COM_BASE_BRAND.MANAGEMENT_UNIT_KEY  → DIM_MANAGEMENT_UNIT.MANAGEMENT_UNIT_KEY
FACT_COM_BASE_BRAND.PRODUCT_KEY          → DIM_PRODUCT.PRODUCT_KEY
FACT_COM_BASE_BRAND.SCENARIO_KEY         → DIM_SCENARIO.SCENARIO_KEY
FACT_COM_BASE_BRAND.TIME_KEY             → DIM_TIME.TIME_KEY

-- Hierarchies
DIM_PRODUCT.PRODUCT_KEY                  → DIM_PRODUCT_HIERARCHY.PRODUCT_KEY
```

---

## Key Design Patterns

### 1. Dual-Fact Architecture
- **FACT_PNL_BASE_BRAND:** Financial P&L analysis with 56 pre-aggregated measures
- **FACT_COM_BASE_BRAND:** Commercial market metrics with simplified structure
- Enables separate optimization for different analytical workloads

### 2. Product Dimension
- **DIM_PRODUCT** serves as the product/brand dimension for both fact tables
- Contains both AZ products and competitor products (AZ_PROD_IND flag)
- Single dimension used consistently across P&L and commercial analysis

### 3. Hierarchical Dimensions
Four hierarchy tables support drill-down and roll-up analysis:
- **Account Hierarchy:** 12 levels (e.g., Revenue → Product Revenue → Product Line)
- **Management Unit Hierarchy:** 10 levels (e.g., Global → Region → Country)
- **Product Hierarchy:** 10 levels (e.g., Total → TA → Therapy Area → Brand)

### 4. Multi-Scenario Planning (P&L Fact)
Supports multiple planning versions:
- **Actual:** Current period results
- **Prior Year (PY_*):** Historical comparison
- **Budget (BUD_*):** Annual plan
- **Mid-Term Plan (MTP_*):** Rolling forecast
- **Long-Term Plan (RBU2LTP_*):** Strategic plan

### 5. Time Intelligence
- **Self-referencing:** PRIOR_YEAR_TIME_KEY enables YoY comparisons
- **Current period flags:** IS_CURRENT_MONTH/QUARTER/YEAR
- **YTD support:** YTD_MONTH_NUMBER for year-to-date calculations

### 6. Pre-Aggregated Metrics (P&L Fact Only)
56 measures pre-calculated for performance:
- **Period aggregations:** QTD, YTD, Q1-Q4, H1-H2
- **Scenario comparisons:** PY_*, BUD_*, MTP_*, RBU2LTP_*
- **Variance calculations:** 9 variance columns

---

## Business Use Cases

### P&L Analysis (FACT_PNL_BASE_BRAND)
1. Financial statement reporting by brand and account
2. Budget vs. actual variance analysis
3. Multi-year trend analysis
4. Quarterly and YTD performance tracking
5. Management unit P&L consolidation
6. Scenario planning and forecasting

### Commercial Analysis (FACT_COM_BASE_BRAND)
1. Market share analysis by product and market
2. Product performance across markets
3. Therapeutic area sales tracking
4. Competitor analysis (AZ vs. non-AZ products via AZ_PROD_IND flag)
5. Geographic market penetration
6. Commercial trend analysis by market segment

### Cross-Functional Analysis
1. P&L impact by market performance
2. Product profitability by market
3. ROI analysis across therapeutic areas
4. Integrated financial and commercial dashboards

---

## Sample Queries

### Query 1: P&L by Brand with Budget Variance
```sql
SELECT 
    ph.LEVEL2_PRODUCT_DESC AS therapeutic_area,
    p.PRODUCT_DESCRIPTION AS brand,
    a.ACCOUNT_DESCRIPTION,
    t.YEAR_NUMBER,
    t.QUARTER_CODE,
    SUM(f.VALUE) AS actual_value,
    SUM(f.BUD_VALUE) AS budget_value,
    SUM(f.BUD_VARIANCE) AS variance,
    ROUND(SUM(f.BUD_VARIANCE) / NULLIF(SUM(f.BUD_VALUE), 0) * 100, 2) AS variance_pct
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_ACCOUNT a ON f.ACCOUNT_KEY = a.ACCOUNT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
LEFT JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT_HIERARCHY ph ON p.PRODUCT_KEY = ph.PRODUCT_KEY
WHERE t.YEAR_NUMBER = 2025
GROUP BY ph.LEVEL2_PRODUCT_DESC, p.PRODUCT_DESCRIPTION, a.ACCOUNT_DESCRIPTION, 
         t.YEAR_NUMBER, t.QUARTER_CODE
ORDER BY variance DESC;
```

### Query 2: Market Share Analysis (Commercial)
```sql
SELECT 
    m.MARKET_NAME,
    m.SUB_MARKET_NAME,
    p.PRODUCT_DESCRIPTION,
    p.AZ_PROD_IND,
    t.YEAR_NUMBER,
    SUM(f.VALUE) AS market_value,
    SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) AS az_value,
    ROUND(SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) / 
          NULLIF(SUM(f.VALUE), 0) * 100, 2) AS az_market_share_pct
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MARKET m ON f.MARKET_KEY = m.MARKET_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
WHERE t.YEAR_NUMBER = 2025
GROUP BY m.MARKET_NAME, m.SUB_MARKET_NAME, p.PRODUCT_DESCRIPTION, p.AZ_PROD_IND, t.YEAR_NUMBER
ORDER BY market_value DESC;
```

### Query 3: Hierarchical Account Rollup
```sql
SELECT 
    ah.LEVEL1_ACCOUNT_DESC AS level1,
    ah.LEVEL2_ACCOUNT_DESC AS level2,
    ah.LEVEL3_ACCOUNT_DESC AS level3,
    a.ACCOUNT_DESCRIPTION AS detail_account,
    t.QUARTER_CODE,
    SUM(f.YTD) AS ytd_value
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_ACCOUNT a ON f.ACCOUNT_KEY = a.ACCOUNT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_ACCOUNT_HIERARCHY ah ON a.ACCOUNT_KEY = ah.ACCOUNT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
WHERE t.IS_CURRENT_QUARTER = TRUE
GROUP BY ah.LEVEL1_ACCOUNT_DESC, ah.LEVEL2_ACCOUNT_DESC, ah.LEVEL3_ACCOUNT_DESC, 
         a.ACCOUNT_DESCRIPTION, t.QUARTER_CODE
ORDER BY ah.LEVEL1_ACCOUNT_SORT_ORDER, ah.LEVEL2_ACCOUNT_SORT_ORDER, ah.LEVEL3_ACCOUNT_SORT_ORDER;
```

---

## Data Lineage & Notes

### Data Volume Summary
| Category | Count | Total Size |
|----------|-------|------------|
| **Total Records** | ~289,000 | ~19 MB |
| **Fact Records** | ~289,000 | ~19 MB |
| **Dimension Records** | 714 | ~23 KB |
| **Hierarchy Records** | 202 | ~42 KB |

### Important Notes
1. **Product Dimension:** Only DIM_PRODUCT table exists (no separate DIM_PRODUCT table)
2. **Time Period:** Data covers 36 months (3 years)
3. **Anonymization:** Numeric values in fact tables may be obfuscated (multiplied by constant factor)
4. **Geographic Scope:** Data limited to Spain and Brazil markets only
5. **AZ Products:** AZ_PROD_IND flag distinguishes company products from competitor products

---

## Schema Maturity

✅ **Production-Ready Features:**
- Star schema design with clear relationships
- Hierarchical dimensions for drill-down analysis
- Pre-aggregated metrics for performance
- Time intelligence with self-referencing
- Multi-scenario support

⚠️ **Considerations:**
- No formal primary/foreign key constraints (Snowflake best practice for performance)
- Hierarchy tables use denormalized structure (12/10 level columns)
- Fact tables lack explicit indexes (Snowflake auto-optimizes)

---

**Document Version:** 2.0  
**Last Updated:** February 27, 2026  
**Schema:** ENT_EAA_PUB.EA_HACKATHON  
**Contact:** Data Engineering & Analytics Team
