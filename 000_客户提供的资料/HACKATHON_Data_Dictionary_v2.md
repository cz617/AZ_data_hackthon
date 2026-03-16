# EA_HACKATHON Data Dictionary

**Database:** ENT_HACKATHON_DATA_SHARE  
**Schema:** EA_HACKATHON  
**Data Period:** January 2023 - December 2025 (3 years, 36 months)  
**Last Updated:** February 27, 2026

---

## Table of Contents
1. [Fact Tables](#fact-tables)
   - [FACT_PNL_BASE_BRAND](#fact_pnl_base_brand)
   - [FACT_COM_BASE_BRAND](#fact_com_base_brand)
2. [Dimension Tables](#dimension-tables)
   - [DIM_ACCOUNT](#dim_account)
   - [DIM_MANAGEMENT_UNIT](#dim_management_unit)
   - [DIM_BRAND / DIM_PRODUCT](#dim_brand--dim_product)
   - [DIM_SCENARIO](#dim_scenario)
   - [DIM_TIME](#dim_time)
   - [DIM_MARKET](#dim_market)
3. [Hierarchy Tables](#hierarchy-tables)
   - [DIM_ACCOUNT_HIERARCHY](#dim_account_hierarchy)
   - [DIM_MANAGEMENT_UNIT_HIERARCHY](#dim_management_unit_hierarchy)
   - [DIM_PRODUCT_HIERARCHY](#dim_product_hierarchy)

---

## Fact Tables

### FACT_PNL_BASE_BRAND
**Purpose:** Central fact table storing Profit & Loss (P&L) metrics by brand with multi-scenario support and pre-calculated aggregations.

**Record Count:** 265,104  
**Size:** 19,091,968 bytes (~19 MB)  
**Grain:** One record per Account-ManagementUnit-Brand-Scenario-Time combination

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **ACCOUNT_KEY** | NUMBER(38,0) | Yes | Foreign key to DIM_ACCOUNT |
| **MANAGEMENT_UNIT_KEY** | NUMBER(38,0) | Yes | Foreign key to DIM_MANAGEMENT_UNIT |
| **BRAND_KEY** | NUMBER(38,0) | Yes | Foreign key to DIM_BRAND (= PRODUCT_KEY) |
| **SCENARIO_KEY** | NUMBER(38,0) | Yes | Foreign key to DIM_SCENARIO |
| **TIME_KEY** | NUMBER(38,0) | Yes | Foreign key to DIM_TIME |
| **VALUE** | NUMBER(38,15) | Yes | Actual/current period financial value |
| **QTD** | NUMBER(38,15) | Yes | Quarter-to-Date accumulated value |
| **YTD** | NUMBER(38,15) | Yes | Year-to-Date accumulated value |
| **Q1** | NUMBER(38,15) | Yes | Quarter 1 total value |
| **Q2** | NUMBER(38,15) | Yes | Quarter 2 total value |
| **Q3** | NUMBER(38,15) | Yes | Quarter 3 total value |
| **Q4** | NUMBER(38,15) | Yes | Quarter 4 total value |
| **H1** | NUMBER(38,15) | Yes | Half 1 (Q1+Q2) total value |
| **H2** | NUMBER(38,15) | Yes | Half 2 (Q3+Q4) total value |
| **PY_VALUE** | NUMBER(38,15) | Yes | Prior Year period value |
| **PY_QTD** | NUMBER(38,15) | Yes | Prior Year Quarter-to-Date value |
| **PY_YTD** | NUMBER(38,15) | Yes | Prior Year Year-to-Date value |
| **PY_Q1** | NUMBER(38,15) | Yes | Prior Year Quarter 1 value |
| **PY_Q2** | NUMBER(38,15) | Yes | Prior Year Quarter 2 value |
| **PY_Q3** | NUMBER(38,15) | Yes | Prior Year Quarter 3 value |
| **PY_Q4** | NUMBER(38,15) | Yes | Prior Year Quarter 4 value |
| **PY_H1** | NUMBER(38,15) | Yes | Prior Year Half 1 value |
| **PY_H2** | NUMBER(38,15) | Yes | Prior Year Half 2 value |
| **PY_QTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: QTD vs Prior Year QTD |
| **PY_YTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: YTD vs Prior Year YTD |
| **BUD_VALUE** | NUMBER(38,15) | Yes | Budget period value |
| **BUD_QTD** | NUMBER(38,15) | Yes | Budget Quarter-to-Date value |
| **BUD_YTD** | NUMBER(38,15) | Yes | Budget Year-to-Date value |
| **BUD_Q1** | NUMBER(38,15) | Yes | Budget Quarter 1 value |
| **BUD_Q2** | NUMBER(38,15) | Yes | Budget Quarter 2 value |
| **BUD_Q3** | NUMBER(38,15) | Yes | Budget Quarter 3 value |
| **BUD_Q4** | NUMBER(38,15) | Yes | Budget Quarter 4 value |
| **BUD_H1** | NUMBER(38,15) | Yes | Budget Half 1 value |
| **BUD_H2** | NUMBER(38,15) | Yes | Budget Half 2 value |
| **BUD_QTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: QTD vs Budget QTD |
| **BUD_YTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: YTD vs Budget YTD |
| **MTP_VALUE** | NUMBER(38,15) | Yes | Mid-Term Plan period value |
| **MTP_QTD** | NUMBER(38,15) | Yes | Mid-Term Plan Quarter-to-Date value |
| **MTP_YTD** | NUMBER(38,15) | Yes | Mid-Term Plan Year-to-Date value |
| **MTP_Q1** | NUMBER(38,15) | Yes | Mid-Term Plan Quarter 1 value |
| **MTP_Q2** | NUMBER(38,15) | Yes | Mid-Term Plan Quarter 2 value |
| **MTP_Q3** | NUMBER(38,15) | Yes | Mid-Term Plan Quarter 3 value |
| **MTP_Q4** | NUMBER(38,15) | Yes | Mid-Term Plan Quarter 4 value |
| **MTP_H1** | NUMBER(38,15) | Yes | Mid-Term Plan Half 1 value |
| **MTP_H2** | NUMBER(38,15) | Yes | Mid-Term Plan Half 2 value |
| **MTP_QTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: QTD vs MTP QTD |
| **MTP_YTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: YTD vs MTP YTD |
| **RBU2LTP_VALUE** | NUMBER(38,15) | Yes | Long-Term Plan (RBU2LTP) period value |
| **RBU2LTP_QTD** | NUMBER(38,15) | Yes | Long-Term Plan Quarter-to-Date value |
| **RBU2LTP_YTD** | NUMBER(38,15) | Yes | Long-Term Plan Year-to-Date value |
| **RBU2LTP_Q1** | NUMBER(38,15) | Yes | Long-Term Plan Quarter 1 value |
| **RBU2LTP_Q2** | NUMBER(38,15) | Yes | Long-Term Plan Quarter 2 value |
| **RBU2LTP_Q3** | NUMBER(38,15) | Yes | Long-Term Plan Quarter 3 value |
| **RBU2LTP_Q4** | NUMBER(38,15) | Yes | Long-Term Plan Quarter 4 value |
| **RBU2LTP_H1** | NUMBER(38,15) | Yes | Long-Term Plan Half 1 value |
| **RBU2LTP_H2** | NUMBER(38,15) | Yes | Long-Term Plan Half 2 value |
| **RBU2LTP_QTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: QTD vs RBU2LTP QTD |
| **RBU2LTP_YTD_VARIANCE** | NUMBER(38,15) | Yes | Variance: YTD vs RBU2LTP YTD |
| **PY_VARIANCE** | NUMBER(38,15) | Yes | Total variance: VALUE vs PY_VALUE |
| **BUD_VARIANCE** | NUMBER(38,15) | Yes | Total variance: VALUE vs BUD_VALUE |
| **RBU2LTP_VARIANCE** | NUMBER(38,15) | Yes | Total variance: VALUE vs RBU2LTP_VALUE |

**Total Columns:** 61 (5 keys + 56 measures)

**Key Features:**
- Pre-aggregated metrics (QTD, YTD, Q1-Q4, H1-H2) for performance optimization
- Support for 4 planning scenarios: Actual, Budget, Mid-Term Plan, Long-Term Plan
- Built-in variance calculations eliminate need for complex joins
- High-precision decimal values (15 decimal places)

---

### FACT_COM_BASE_BRAND
**Purpose:** Commercial/market performance metrics by product and market

**Record Count:** 23,568  
**Size:** 142,336 bytes (~142 KB)  
**Grain:** One record per Account-Market-ManagementUnit-Product-Scenario-Time combination  
**Data Period:** January 2023 - December 2025 (36 months)

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **ACCOUNT_KEY** | NUMBER | Yes | Foreign key to DIM_ACCOUNT |
| **MARKET_KEY** | NUMBER | Yes | Foreign key to DIM_MARKET |
| **MANAGEMENT_UNIT_KEY** | NUMBER | Yes | Foreign key to DIM_MANAGEMENT_UNIT |
| **PRODUCT_KEY** | NUMBER | Yes | Foreign key to DIM_PRODUCT |
| **SCENARIO_KEY** | NUMBER | Yes | Foreign key to DIM_SCENARIO |
| **TIME_KEY** | NUMBER | Yes | Foreign key to DIM_TIME |
| **VALUE** | NUMBER | Yes | Commercial metric value |

**Total Columns:** 7 (6 keys + 1 measure)

**Key Features:**
- Simplified structure focused on market analysis
- Links products to specific market segments
- Supports market share and penetration analysis
- Complementary to P&L fact table

---

## Dimension Tables

### DIM_ACCOUNT
**Purpose:** Master list of P&L account line items (e.g., Revenue, COGS, Operating Expenses)

**Record Count:** 22  
**Size:** 2,048 bytes  
**Type:** Dimension (Type 1 - Slowly Changing)

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **ACCOUNT_KEY** | NUMBER(38,0) | No | Primary key / Unique identifier |
| **ACCOUNT_CODE** | TEXT | No | Business code for the account (e.g., "MARKET_SHARE") |
| **ACCOUNT_DESCRIPTION** | TEXT | Yes | Human-readable account name (e.g., "Market Share") |

**Business Rules:**
- Contains chart of accounts for P&L reporting
- Includes both summary and detail level accounts
- Used to categorize financial transactions
- 23 accounts including revenue, expenses, and market metrics

**Sample Query:**
```sql
SELECT ACCOUNT_CODE, ACCOUNT_DESCRIPTION 
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_ACCOUNT 
ORDER BY ACCOUNT_CODE;
```

---

### DIM_MANAGEMENT_UNIT
**Purpose:** Organizational hierarchy dimension representing business units or management structures

**Record Count:** 2  
**Size:** 1,536 bytes  
**Type:** Dimension (Type 1 - Slowly Changing)

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **MANAGEMENT_UNIT_KEY** | NUMBER(38,0) | No | Primary key / Unique identifier |
| **COMPOUND_MANAGEMENT_UNIT_CODE** | TEXT | No | Business code (e.g., "44000ES", "44000BR") |
| **MANAGEMENT_UNIT_NAME** | TEXT | Yes | Unit name ("Spain" or "Brazil") |

**Business Rules:**
- Only 2 management units: Spain (44000ES) and Brazil (44000BR)
- Represents geographic markets/regions
- Used to aggregate P&L by organizational responsibility
- Links to 10-level hierarchy for drill-down analysis

**Sample Query:**
```sql
SELECT COMPOUND_MANAGEMENT_UNIT_CODE, MANAGEMENT_UNIT_NAME 
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MANAGEMENT_UNIT 
ORDER BY MANAGEMENT_UNIT_KEY;
```

---

### DIM_PRODUCT
**Purpose:** Master list of product brands tracked in P&L and commercial analysis

**Record Count:** 614  
**Size:** 12,288 bytes  
**Type:** Dimension (Type 1 - Slowly Changing)

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **PRODUCT_KEY** | NUMBER(38,0) | No | Primary key / Unique identifier |
| **PRODUCT_CODE** | TEXT | No | Business code for the product (e.g., "PRAZOL") |
| **PRODUCT_DESCRIPTION** | TEXT | No | Full product/brand name (e.g., "EFFIENT", "MICARDIS ANLO") |
| **AZ_PROD_IND** | BOOLEAN | Yes | TRUE = AstraZeneca product, FALSE/0 = Competitor product |

**Business Rules:**
- Represents pharmaceutical or product brands
- Core dimension for brand-level P&L and market analysis
- AZ_PROD_IND flag distinguishes company products from competitors
- All products have both code and name populated
- 614 products spanning multiple therapeutic areas
- Products available in Spain and Brazil markets
- **NOTE:** There is no separate DIM_BRAND table - this single table serves both P&L and commercial facts

**Sample Query:**
```sql
SELECT 
    PRODUCT_CODE, 
    PRODUCT_DESCRIPTION,
    CASE WHEN AZ_PROD_IND = TRUE THEN 'AZ Product' ELSE 'Competitor' END AS product_type
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT 
ORDER BY PRODUCT_DESCRIPTION;
```

---

### DIM_SCENARIO
**Purpose:** Defines different planning and reporting scenarios for financial analysis

**Record Count:** 4  
**Size:** 1,536 bytes  
**Type:** Dimension (Type 1 - Static)

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **SCENARIO_KEY** | NUMBER(38,0) | No | Primary key / Unique identifier |
| **SCENARIO_NAME** | TEXT | No | Scenario type (e.g., "Actual", "Budget", "Forecast") |

**Business Rules:**
- Supports multi-scenario financial planning and analysis
- Small static dimension (4 scenarios)
- Critical for variance reporting

**Expected Scenarios:**
- Actual: Real financial results
- Budget: Annual planned targets
- Mid-Term Plan (MTP): Medium-range forecast
- Long-Term Plan (RBU2LTP): Strategic long-range plan

---

### DIM_TIME
**Purpose:** Calendar dimension supporting time-based analysis with fiscal period intelligence

**Record Count:** 36  
**Size:** 4,608 bytes  
**Type:** Dimension (Type 1 - Slowly Changing)  
**Data Period:** January 2023 - December 2025

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **TIME_KEY** | NUMBER(6,0) | No | Primary key (format: YYYYMM, e.g., 202504) |
| **DATE_KEY** | DATE | No | Actual calendar date for the period |
| **MONTH_NUMBER** | NUMBER(2,0) | No | Month number (1-12) |
| **MONTH_NAME** | TEXT | No | Month name (e.g., "January", "February") |
| **QUARTER_CODE** | TEXT | No | Quarter code (e.g., "2025-Q1") |
| **QUARTER_NUMBER** | NUMBER(1,0) | No | Quarter number (1-4) |
| **YEAR_NUMBER** | NUMBER(4,0) | No | Four-digit year (e.g., 2025) |
| **YTD_MONTH_NUMBER** | NUMBER(2,0) | No | Month position in year for YTD calculations (1-12) |
| **PRIOR_YEAR_TIME_KEY** | NUMBER(6,0) | Yes | Self-referencing key to same period in prior year |
| **IS_CURRENT_MONTH** | BOOLEAN | No | Flag: TRUE if this is the current month |
| **IS_CURRENT_QUARTER** | BOOLEAN | No | Flag: TRUE if this is the current quarter |
| **IS_CURRENT_YEAR** | BOOLEAN | No | Flag: TRUE if this is the current year |

**Business Rules:**
- Covers 36 months: January 2023 through December 2025
- Includes all 12 months for each of the 3 years (2023, 2024, 2025)
- Self-referencing design via PRIOR_YEAR_TIME_KEY enables easy YoY comparisons
- Current period flags enable dynamic "as of today" reporting
- TIME_KEY follows YYYYMM format (e.g., 202301 = January 2023, 202512 = December 2025)

**Key Features:**
- **Time Intelligence:** Built-in prior year linkage
- **Current Period Tracking:** Boolean flags for current month/quarter/year
- **Hierarchical:** Supports drill-down from Year → Quarter → Month
- **YTD Calculations:** YTD_MONTH_NUMBER supports year-to-date logic

**Sample Queries:**

```sql
-- Get current month data
SELECT * FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME 
WHERE IS_CURRENT_MONTH = TRUE;

-- Find prior year period
SELECT 
    t1.TIME_KEY as current_period,
    t1.MONTH_NAME,
    t2.TIME_KEY as prior_year_period
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t1
LEFT JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t2 
    ON t1.PRIOR_YEAR_TIME_KEY = t2.TIME_KEY
WHERE t1.YEAR_NUMBER = 2025;
```

---

### DIM_MARKET
**Purpose:** Market and therapeutic category classification

**Record Count:** 26  
**Size:** 2,560 bytes  
**Type:** Dimension (Type 1 - Slowly Changing)

#### Columns

| Column Name | Data Type | Nullable | Description |
|-------------|-----------|----------|-------------|
| **MARKET_KEY** | NUMBER | Yes | Primary key / Unique identifier |
| **MARKET_NAME** | TEXT | Yes | Market name (e.g., "ORAL ANTI-PLATELET", "BETA BLOCKERS") |
| **SUB_MARKET_NAME** | TEXT | Yes | Sub-market or segment name |

**Business Rules:**
- 26 distinct therapeutic markets/categories
- Supports market segmentation analysis
- Used exclusively with FACT_COM_BASE_BRAND
- Enables market share and penetration tracking

**Sample Markets:**
- Oncology: EGFR TKI, PARP INHIBITORS, ADVANCED BREAST CANCER
- Cardiovascular: ORAL ANTI-PLATELET, BETA BLOCKERS, HIGH BLOOD PRESSURE
- Respiratory: ICS/LAMA/LABA, PDE4 INHIBITORS

**Sample Query:**
```sql
SELECT MARKET_NAME, SUB_MARKET_NAME
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MARKET
ORDER BY MARKET_NAME;
```

---

## Hierarchy Tables

### DIM_ACCOUNT_HIERARCHY
**Purpose:** 12-level account rollup structure for P&L reporting

**Record Count:** 23  
**Size:** 12,800 bytes  
**Type:** Hierarchy (Denormalized)

#### Column Structure

| Column Pattern | Type | Description |
|----------------|------|-------------|
| HIERARCHY_GROUP_NAME | TEXT | Hierarchy type (e.g., "pnl_group_hierarchy_set_internal_display") |
| HIERARCHY_GROUP_CODE | TEXT | Hierarchy code |
| LEVEL1_ACCOUNT_CODE | TEXT | Level 1 account code |
| LEVEL1_ACCOUNT_DESC | TEXT | Level 1 account description |
| LEVEL1_ACCOUNT_SORT_ORDER | NUMBER | Level 1 sort order |
| ... (repeat for LEVEL2-LEVEL12) | ... | 12 levels total, each with CODE/DESC/SORT_ORDER |
| ACCOUNT_CODE | TEXT | Leaf account code (links to DIM_ACCOUNT) |
| ACCOUNT_GROUP | TEXT | Account grouping |
| ACCOUNT_KEY | NUMBER | FK to DIM_ACCOUNT |

**Total Columns:** 41 (12 levels × 3 attributes + 3 base columns)

**Business Rules:**
- Denormalized structure for query performance
- Each level has code, description, and sort order
- Supports drill-down from high-level P&L categories to detail accounts
- Sort orders enable proper financial statement ordering

**Example Hierarchy:**
```
Core Operating Profit (Level 1)
  └─ SG&A (Level 2)
      └─ SMM (Level 3)
          └─ Sales (Level 4)
              └─ Sales (Level 5 - detail)
```

---

### DIM_MANAGEMENT_UNIT_HIERARCHY
**Purpose:** 10-level organizational rollup hierarchy

**Record Count:** 2  
**Size:** 9,216 bytes  
**Type:** Hierarchy (Denormalized)

#### Column Structure

| Column Pattern | Type | Description |
|----------------|------|-------------|
| HIERARCHY_GROUP_NAME | TEXT | Hierarchy type |
| HIERARCHY_GROUP_CODE | TEXT | Hierarchy code |
| LEVEL1_MANAGEMENT_UNIT_CODE | TEXT | Level 1 unit code |
| LEVEL1_MANAGEMENT_UNIT_DESC | TEXT | Level 1 unit description |
| LEVEL1_MANAGEMENT_UNIT_SORT_ORDER | NUMBER | Level 1 sort order |
| ... (repeat for LEVEL2-LEVEL10) | ... | 10 levels total, each with CODE/DESC/SORT_ORDER |
| MANAGEMENT_UNIT_CODE | TEXT | Leaf unit code |
| MANAGEMENT_UNIT_GROUP | TEXT | Unit grouping |
| MANAGEMENT_UNIT_KEY | NUMBER | FK to DIM_MANAGEMENT_UNIT |

**Total Columns:** 35 (10 levels × 3 attributes + 3 base columns)

**Business Rules:**
- Only 2 management unit hierarchies (Spain and Brazil)
- Supports organizational drill-down and roll-up
- Enables reporting by region/country
- 10 levels support complex organizational structures

---

### DIM_PRODUCT_HIERARCHY
**Purpose:** 10-level product taxonomy (therapeutic area structure)

**Record Count:** 174  
**Size:** 19,968 bytes  
**Type:** Hierarchy (Denormalized)

#### Column Structure

| Column Pattern | Type | Description |
|----------------|------|-------------|
| HIERARCHY_GROUP_NAME | TEXT | Hierarchy type (e.g., "Total_Product_Management_Reporting") |
| HIERARCHY_GROUP_CODE | TEXT | Hierarchy code |
| LEVEL1_PRODUCT_CODE | TEXT | Level 1 product code (e.g., "Total Product (Reporting)") |
| LEVEL1_PRODUCT_DESC | TEXT | Level 1 description |
| LEVEL1_PRODUCT_SORT_ORDER | NUMBER | Level 1 sort order |
| LEVEL2_PRODUCT_DESC | TEXT | Level 2 (e.g., "Oncology TA", "BioPharma TA") |
| LEVEL3_PRODUCT_DESC | TEXT | Level 3 (therapy area detail) |
| ... (repeat for LEVEL4-LEVEL10) | ... | 10 levels total |
| PRODUCT_CODE | TEXT | Leaf product code |
| PRODUCT_GROUP | TEXT | Product grouping |
| PRODUCT_KEY | NUMBER | FK to DIM_PRODUCT |

**Total Columns:** 35 (10 levels × 3 attributes + 3 base columns)

**Business Rules:**
- 174 unique product hierarchy paths
- Supports therapeutic area (TA) analysis
- Enables portfolio management reporting
- Links products to strategic business segments

**Example Hierarchy:**
```
Total Product (Reporting) (Level 1)
  ├─ Oncology TA (Level 2)
  │   └─ Datroway (Level 3)
  ├─ BioPharma TA (Level 2)
  │   └─ BioPharma: CVRM (Level 3)
  ├─ Rare Disease TA (Level 2)
  │   └─ Other RD Brands (Level 3)
  └─ Central TA (Level 2)
      └─ Central Brands (Level 3)
```

---

## Naming Conventions

### General Patterns
- **Primary Keys:** `<TABLE>_KEY` (e.g., BRAND_KEY, ACCOUNT_KEY)
- **Code Columns:** `<ENTITY>_CODE` (business identifiers)
- **Description/Name Columns:** `<ENTITY>_NAME` or `<ENTITY>_DESCRIPTION`
- **Measure Columns:** Descriptive names (VALUE, QTD, YTD)
- **Flag Columns:** `IS_<CONDITION>` (boolean flags)

### Measure Prefixes
- **PY_** = Prior Year
- **BUD_** = Budget
- **MTP_** = Mid-Term Plan
- **RBU2LTP_** = Long-Term Plan (Rolling Business Unit 2 Long-Term Plan)

### Aggregation Suffixes
- **_VALUE** = Period value
- **_QTD** = Quarter-to-Date
- **_YTD** = Year-to-Date
- **_Q1/Q2/Q3/Q4** = Quarterly totals
- **_H1/H2** = Half-year totals
- **_VARIANCE** = Variance calculation

---

## Data Types Summary

| Data Type | Usage | Description |
|-----------|-------|-------------|
| NUMBER(38,0) | Keys | Surrogate/primary keys and foreign keys |
| NUMBER(38,15) | Measures | Financial values with high precision (15 decimal places) |
| NUMBER(6,0) | Time Keys | YYYYMM format (e.g., 202504) |
| NUMBER(2,0) | Month | Month number (1-12) |
| NUMBER(1,0) | Quarter | Quarter number (1-4) |
| NUMBER(4,0) | Year | Four-digit year |
| TEXT | Codes/Names | Variable-length strings (Snowflake TEXT = VARCHAR(16777216)) |
| DATE | Dates | Calendar dates |
| BOOLEAN | Flags | True/False indicators |

---

## Relationships Summary

```
DIM_ACCOUNT (22) ────────┐
                         │
DIM_MANAGEMENT_UNIT (2)──┼──→ FACT_PNL_BASE_BRAND (265,104)
                         │
DIM_PRODUCT (614) ───────┤
                         │
DIM_SCENARIO (4) ────────┤
                         │
DIM_TIME (36) ───────────┘


DIM_ACCOUNT (22) ────────┐
                         │
DIM_MARKET (26) ─────────┤
                         │
DIM_MANAGEMENT_UNIT (2)──┼──→ FACT_COM_BASE_BRAND (23,568)
                         │
DIM_PRODUCT (614) ───────┤
                         │
DIM_SCENARIO (4) ────────┤
                         │
DIM_TIME (36) ───────────┘


Hierarchies:
DIM_ACCOUNT → DIM_ACCOUNT_HIERARCHY (12 levels)
DIM_MANAGEMENT_UNIT → DIM_MANAGEMENT_UNIT_HIERARCHY (10 levels, 2 units)
DIM_PRODUCT → DIM_PRODUCT_HIERARCHY (10 levels)
```

**Cardinality:**
- Each dimension has a 1:many relationship with fact tables
- No inter-dimension relationships (classic star schema)
- Only DIM_PRODUCT exists (no separate DIM_BRAND table)

---

## Usage Guidelines

### Best Practices
1. **Always use fully qualified names:** `ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.<TABLE>`
2. **Filter early:** Apply dimension filters before aggregating facts
3. **Leverage pre-aggregated columns:** Use QTD/YTD columns instead of recalculating (P&L fact only)
4. **Use time flags:** Leverage IS_CURRENT_* flags for dynamic date filtering
5. **Variance analysis:** Use built-in variance columns for performance (P&L fact)
6. **Product dimension:** Use DIM_PRODUCT for both P&L and commercial analysis

### Common Query Patterns

```sql
-- Pattern 1: Brand P&L with YoY comparison
SELECT 
    b.PRODUCT_DESCRIPTION AS product_name,
    a.ACCOUNT_DESCRIPTION,
    t.MONTH_NAME,
    f.VALUE as current_value,
    f.PY_VALUE as prior_year_value,
    f.PY_VARIANCE as variance
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT b ON f.PRODUCT_KEY = b.PRODUCT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_ACCOUNT a ON f.ACCOUNT_KEY = a.ACCOUNT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
WHERE t.IS_CURRENT_YEAR = TRUE;

-- Pattern 2: Budget vs Actual Analysis
SELECT 
    mu.MANAGEMENT_UNIT_NAME,
    SUM(f.YTD) as actual_ytd,
    SUM(f.BUD_YTD) as budget_ytd,
    SUM(f.BUD_YTD_VARIANCE) as variance_ytd
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MANAGEMENT_UNIT mu 
    ON f.MANAGEMENT_UNIT_KEY = mu.MANAGEMENT_UNIT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
WHERE t.IS_CURRENT_MONTH = TRUE
GROUP BY mu.MANAGEMENT_UNIT_NAME;

-- Pattern 3: Market Share Analysis
SELECT 
    m.MARKET_NAME,
    p.PRODUCT_DESCRIPTION,
    p.AZ_PROD_IND,
    SUM(f.VALUE) AS total_value,
    SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) / SUM(f.VALUE) * 100 AS az_share_pct
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MARKET m ON f.MARKET_KEY = m.MARKET_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
GROUP BY m.MARKET_NAME, p.PRODUCT_DESCRIPTION, p.AZ_PROD_IND;
```

---

## Metadata Information

| Attribute | Value |
|-----------|-------|
| **Database** | ENT_EAA_PUB |
| **Schema** | EA_HACKATHON |
| **Total Tables** | 11 |
| **Total Records** | ~289,000 |
| **Total Size** | ~19 MB |
| **Model Type** | Dual-Fact Star Schema |
| **Domain** | Finance P&L + Commercial Analytics |
| **Time Coverage** | 36 months (3 years) |
| **Geographic Scope** | Spain & Brazil only |
| **Grain (P&L)** | Account-ManagementUnit-Brand-Scenario-Month |
| **Grain (Commercial)** | Account-Market-ManagementUnit-Product-Scenario-Month |

---

## Glossary

| Term | Definition |
|------|------------|
| **P&L** | Profit & Loss statement |
| **QTD** | Quarter-to-Date: accumulated value from start of quarter to current period |
| **YTD** | Year-to-Date: accumulated value from start of year to current period |
| **MTP** | Mid-Term Plan: medium-range financial forecast |
| **RBU2LTP** | Rolling Business Unit 2 Long-Term Plan: strategic long-range planning scenario |
| **Variance** | Difference between actual and comparison value (budget, prior year, forecast) |
| **Grain** | Level of detail represented by one fact table record |
| **Star Schema** | Data warehouse design with central fact table surrounded by dimension tables |
| **TA** | Therapeutic Area (e.g., Oncology, Cardiovascular, Respiratory) |
| **AZ** | AstraZeneca |
| **CVRM** | Cardiovascular, Renal, and Metabolism |

---

**Document Control:**
- **Created:** February 27, 2026
- **Version:** 2.0
- **Contact:** Data Engineering & Analytics Team
- **Schema Owner:** EDP_ENT_EAA_SYSADMIN_DEV
