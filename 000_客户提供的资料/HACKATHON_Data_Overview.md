# Hackathon - Dataset Overview

This document provides a quick start guide to the **HACKATHON dataset** - your sandbox for analytics, visualization, and AI/ML innovation.

---

## 📊 What Data is Available?

### Financial Data (P&L)
**Table:** `FACT_PNL_BASE_BRAND`

- **265,000+ records** of profit & loss metrics
- **22 P&L accounts** (Revenue, COGS, Operating Expenses, Market Share, etc.)
- **614 brands/products** across multiple therapeutic areas
- **Multi-scenario planning:**
  - Actual results
  - Budget/plan targets
  - Prior year comparisons
  - Mid-term forecast (MTP)
  - Long-term plan (RBU2LTP)
- **Time series:** 36 months (January 2023 - December 2025) of monthly data
- **Pre-calculated metrics:** QTD, YTD, quarterly (Q1-Q4), half-yearly (H1-H2)
- **Built-in variances:** 9 variance columns comparing scenarios

### Commercial & Market Data
**Table:** `FACT_COM_BASE_BRAND`

- **23,500+ records** of market performance
- **26 therapeutic markets** including:
  - Oncology: EGFR TKI, PARP Inhibitors, Advanced Breast Cancer
  - Cardiovascular: Anti-Platelet, Beta Blockers, High Blood Pressure
  - Respiratory: ICS/LAMA/LABA, PDE4 Inhibitors
  - Immunology & Rare Disease markets
- **491 active products** in markets
- **Competitor tracking:** AZ vs. non-AZ products (AZ_PROD_IND flag)
- Market share and penetration metrics

### Organizational Context

- **2 management units/regions:**
  - Spain (44000ES)
  - Brazil (44000BR)
- **Hierarchical structures** for drill-down analysis:
  - 12-level account hierarchy
  - 10-level organizational hierarchy
  - 10-level product taxonomy by therapeutic area

---

## 🔍 Quick Data Exploration

### 1. See All Available Tables
```sql
SHOW TABLES IN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON;
```

**You'll find:**
- 2 Fact tables (FACT_PNL_BASE_BRAND, FACT_COM_BASE_BRAND)
- 6 Dimension tables (DIM_ACCOUNT, DIM_BRAND/PRODUCT, DIM_MARKET, etc.)
- 3 Hierarchy tables (for drill-down analysis)

### 2. Explore P&L Financial Data
```sql
SELECT 
    a.ACCOUNT_DESCRIPTION,
    p.PRODUCT_DESCRIPTION AS brand,
    t.YEAR_NUMBER,
    t.QUARTER_CODE,
    SUM(f.VALUE) AS actual_value,
    SUM(f.BUD_VALUE) AS budget_value,
    SUM(f.BUD_VARIANCE) AS variance
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_PNL_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_ACCOUNT a ON f.ACCOUNT_KEY = a.ACCOUNT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
WHERE t.IS_CURRENT_YEAR = TRUE
GROUP BY 1, 2, 3, 4
ORDER BY variance DESC
LIMIT 100;
```

### 3. Explore Market/Commercial Data
```sql
SELECT 
    m.MARKET_NAME,
    m.SUB_MARKET_NAME,
    p.PRODUCT_DESCRIPTION,
    CASE WHEN p.AZ_PROD_IND = TRUE THEN 'AZ Product' ELSE 'Competitor' END AS product_type,
    COUNT(*) AS record_count,
    SUM(f.VALUE) AS total_value
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MARKET m ON f.MARKET_KEY = m.MARKET_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
GROUP BY 1, 2, 3, 4
ORDER BY total_value DESC
LIMIT 100;
```

### 4. Market Share Analysis
```sql
SELECT 
    m.MARKET_NAME,
    t.YEAR_NUMBER,
    SUM(f.VALUE) AS total_market_value,
    SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) AS az_value,
    ROUND(
        SUM(CASE WHEN p.AZ_PROD_IND = TRUE THEN f.VALUE ELSE 0 END) / 
        NULLIF(SUM(f.VALUE), 0) * 100, 
        2
    ) AS az_market_share_pct
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.FACT_COM_BASE_BRAND f
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_MARKET m ON f.MARKET_KEY = m.MARKET_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON f.PRODUCT_KEY = p.PRODUCT_KEY
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_TIME t ON f.TIME_KEY = t.TIME_KEY
GROUP BY m.MARKET_NAME, t.YEAR_NUMBER
ORDER BY az_market_share_pct DESC;
```

### 5. Product Hierarchy (Therapeutic Areas)
```sql
SELECT 
    ph.LEVEL1_PRODUCT_DESC AS total_portfolio,
    ph.LEVEL2_PRODUCT_DESC AS therapeutic_area,
    ph.LEVEL3_PRODUCT_DESC AS therapy_detail,
    p.PRODUCT_DESCRIPTION AS brand_name
FROM ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT_HIERARCHY ph
JOIN ENT_HACKATHON_DATA_SHARE.EA_HACKATHON.DIM_PRODUCT p ON ph.PRODUCT_KEY = p.PRODUCT_KEY
ORDER BY ph.LEVEL2_PRODUCT_DESC, p.PRODUCT_DESCRIPTION
LIMIT 50;
```

---

## 📚 Additional Resources

### Complete Documentation Available:

1. **Data Model (HACKATHON_Data_Model_v2.md)**
   - Comprehensive schema diagrams
   - Table relationships
   - Design patterns
   - Sample queries

2. **Data Dictionary (HACKATHON_Data_Dictionary_v2.md)**
   - Complete column definitions (61 columns in P&L fact!)
   - Data types and descriptions
   - Business rules
   - Usage guidelines

3. **This Document (HACKATHON_Data_Overview.md)**
   - Quick start guide
   - Use case ideas
   - Common queries

---

## 📋 Table Quick Reference

| Table | Records | Purpose |
|-------|---------|---------|
| **FACT_PNL_BASE_BRAND** | 265,104 | P&L financial metrics (56 measures) |
| **FACT_COM_BASE_BRAND** | 23,568 | Commercial/market metrics |
| **DIM_ACCOUNT** | 22 | P&L accounts |
| **DIM_PRODUCT** | 614 | Products |
| **DIM_MARKET** | 26 | Therapeutic markets |
| **DIM_MANAGEMENT_UNIT** | 2 | Regions (Spain & Brazil) |
| **DIM_SCENARIO** | 4 | Planning scenarios |
| **DIM_TIME** | 36 | 3 years monthly calendar |
| **DIM_ACCOUNT_HIERARCHY** | 23 | 12-level account rollup |
| **DIM_PRODUCT_HIERARCHY** | 174 | 10-level product taxonomy |
| **DIM_MANAGEMENT_UNIT_HIERARCHY** | 2 | 10-level org structure |

---

## 🆘 Need Help?

### Getting Started
1. Run the exploration queries above
2. Review the Data Model document for schema understanding
3. Check the Data Dictionary for column details
4. Start small - test with LIMIT 100

### Common Questions

**Q: Where's the revenue?**  
A: Check DIM_ACCOUNT table - revenue accounts are in ACCOUNT_DESCRIPTION

**Q: How do I calculate market share?**  
A: Use AZ_PROD_IND flag: `SUM(CASE WHEN AZ_PROD_IND=TRUE...) / SUM(VALUE)`

**Q: What's the difference between BRAND and PRODUCT?**  
A: There is only DIM_PRODUCT - no separate DIM_BRAND table exists!

**Q: How do I drill down by therapeutic area?**  
A: Use DIM_PRODUCT_HIERARCHY - LEVEL2 typically has TA

**Q: Where are the variances calculated?**  
A: Already in FACT_PNL_BASE_BRAND - use BUD_VARIANCE, PY_VARIANCE columns

### Contact
- **Hackathon Organizers:** For questions and support
- **Documentation:** Review the full Data Model and Data Dictionary
- **Snowflake Docs:** [docs.snowflake.com](https://docs.snowflake.com)

---

**Good luck with the hackathon! 🎉**

Build something amazing with this data!

---

**Document Information:**
- **Created:** February 27, 2026
- **Version:** 1.0
- **Geographic Scope:** Spain & Brazil only
- **Data Period:** January 2023 - December 2025 (36 months)
