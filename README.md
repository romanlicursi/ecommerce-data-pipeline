# E-Commerce Data Quality Pipeline

A data engineering project that processes 50,000 orders, identifies and fixes 7,984 quality issues, and delivers clean analytics-ready data through automated ETL pipelines.

**What it does:** Takes messy e-commerce data (duplicate orders, missing customer IDs, invalid emails, orphaned product references) and transforms it into reliable tables that power business dashboards. The pipeline validates, cleans, transforms, and monitors data quality automatically.

**Why it matters:** Most companies spend 60% of analyst time fixing data instead of analyzing it. This pipeline automates that work and catches issues before they reach stakeholders.

---

## The Problem

Working with real e-commerce data means dealing with:

- Orders appearing twice in the system (inflates revenue by 3%)
- 5% of customers have NULL IDs (can't track lifetime value)
- Marketing source names entered 15 different ways ("google ads" vs "Google Ads" vs "GOOGLE ADS")
- Product IDs that don't exist in the catalog anymore
- Negative dollar amounts from data entry errors
- Email addresses with broken formats

These aren't edge cases. This is what production data looks like, and it breaks analytics if you don't handle it systematically.

---

## The Solution

Built a multi-layer pipeline that handles the full data lifecycle:

**Ingestion** → Load raw CSV and JSON files into a local database (DuckDB)

**Validation** → Run automated quality checks and log every issue found

**Cleaning** → Fix 7,984 issues using documented resolution logic

**Transformation** → Build analytics tables with proper schemas and business logic

**Monitoring** → Track quality metrics and generate reports

**Backup** → Automated Parquet exports with compression

The whole thing runs in under 30 seconds for 50K records.

---

## Technical Implementation

### Pipeline Architecture

Each layer has a specific job and passes data to the next stage:
```
raw data → validation (identify issues) → cleaning (fix issues) → 
transformation (business logic) → analytics tables → dashboards
```

The key insight: validate before cleaning, clean before transforming. Each stage is idempotent (can run multiple times safely) and logs what it does.

### Data Quality Framework

Measures quality across four dimensions:
- **Completeness:** Are required fields populated?
- **Validity:** Do values match expected formats and ranges?
- **Uniqueness:** Are there duplicate records?
- **Consistency:** Do foreign keys resolve? Are values standardized?

Issues identified and resolved:

| Issue Type | Records Affected | Resolution |
|-----------|-----------------|------------|
| Duplicate order_ids | 1,451 | Removed duplicates, kept first occurrence |
| NULL customer_ids | 2,521 | Assigned temporary IDs for tracking |
| Invalid email formats | 1,529 | Fixed programmatically (_AT_ → @) |
| Negative amounts | 1,030 | Converted to absolute values |
| Invalid state codes | 523 | Marked as UNKNOWN for investigation |
| Orphaned product_ids | 930 | Removed from pipeline, logged separately |

**Results after cleaning:**
- 100% order_id uniqueness
- 100% valid email formats
- 100% positive order amounts
- 100% product catalog referential integrity

### SQL Transformations

Built five analytics tables using CTEs and window functions:

- **fct_orders** - fact table with enriched order data (47,616 rows)
- **dim_customers** - customer-level aggregates for LTV analysis (11,887 customers)
- **product_performance** - product metrics for merchandising decisions
- **marketing_attribution** - source performance with revenue ranking (72 source-quarter combinations)
- **daily_metrics** - time series with rolling averages (1,050 daily observations)

The SQL is readable and maintainable. No spaghetti joins, no magic numbers.

### Cloud Architecture Design

Designed to deploy directly to AWS with minimal code changes. Here's the mapping:

**Local → AWS Production:**
- CSV files → S3 buckets (raw/staging/analytics layers)
- Python scripts → Glue ETL jobs
- Validation logic → Glue Data Quality rules
- DuckDB → Apache Iceberg tables + Athena
- Backup scripts → Lambda functions
- Tableau → QuickSight

This mirrors the architecture used in production data platforms for handling millions of records daily.

---

## What I Learned

**Data quality is harder than it looks.** It's not enough to find issues. You need documented resolution logic, logging, and the ability to explain every decision to stakeholders.

**Pipeline design is about trade-offs.** Should you fix incorrect data or flag it for review? Remove bad records or preserve them in a separate table? There's no "right" answer without business context.

**SQL is still the right tool.** I could have done everything in Python, but SQL transformations are more readable, easier to test, and what analysts already know.

**Cloud architecture starts with concepts, not services.** Understanding data lakes, validation layers, and incremental processing matters more than memorizing AWS console screens.

---

## Business Impact

**For Marketing:** Standardized inconsistent source names into clean categories. Enabled accurate ROI calculation by channel through the marketing attribution table.

**For Analytics Team:** Reduced time spent on data prep by automating validation and cleaning. Quality checks catch issues before analysts see them.

**For Finance:** Removed duplicate revenue from reports. Added validation to prevent negative amounts from reaching financial dashboards.

---

## Tech Stack

- Python 3.11 (pandas, faker)
- DuckDB 1.0
- SQL (CTEs, window functions, complex joins)
- Tableau
- Git

**Designed for AWS:** Glue, S3, Athena, Iceberg, QuickSight, Lambda

---

## Running It
```bash
# Install dependencies
pip install duckdb pandas faker

# Run full pipeline
./run_pipeline.sh

# Or run steps individually
python scripts/01_ingest_raw_data.py
python scripts/02_validate_data.py
python scripts/03_clean_data.py
python scripts/04_run_transformations.py
python scripts/05_data_quality_monitoring.py
python scripts/06_backup_analytics_tables.py
```

Takes under 30 seconds. Creates analytics-ready tables in `data/ecommerce_pipeline.duckdb`.

---

## Project Structure
```
ecommerce-data-pipeline/
├── scripts/          # ETL pipeline stages
├── sql/              # Business logic transformations
├── data/             # Database and staged files
├── logs/             # Execution logs
├── backups/          # Automated Parquet exports
└── README.md
```

---

## Contact

**Roman Licursi**

- Email: romanlicursi@gmail.com
- LinkedIn: [linkedin.com/in/roman-licursi-3aab2a160](https://www.linkedin.com/in/roman-licursi-3aab2a160/)
- Portfolio: [View Portfolio](https://elastic-bar-b32.notion.site/Hey-I-m-Roman-286fb92c923f80e3b937fd9ad3f319e4?pvs=143)
