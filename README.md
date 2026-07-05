# CMS Medicare Provider Analytics Pipeline

A production-grade batch data pipeline built on Azure Databricks that ingests, transforms, and aggregates 9.7 million CMS Medicare provider records to analyze healthcare cost variation and hospital readmission risk across California providers.

## Business Questions Answered
- Which California providers have the highest cost variation for common procedures?
- Which Orange County hospitals have the highest readmission risk by condition?

## Architecture

```
CMS Public Data (data.cms.gov)
        ↓ Manual upload (simulating scheduled arrival)
ADLS Gen2 (cms-medicare-raw container)
        ↓ Auto Loader (cloudFiles)
Bronze Layer — Raw, append-only, 9.7M rows, all strings
        ↓ PySpark transformations + data quality checks
Silver Layer — Typed, cleaned, California-filtered
        ↓ Aggregation + business logic
Gold Layer — Provider cost scorecard + Hospital readmission risk
        ↓ Lakeflow Jobs (daily schedule)
Unity Catalog — Governance, lineage, access control
```

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (Premium, Hybrid) |
| Table Format | Delta Lake |
| Ingestion | Auto Loader (cloudFiles) |
| Language | PySpark + Spark SQL |
| Governance | Unity Catalog |
| Orchestration | Lakeflow Jobs |
| CI/CD | Databricks Asset Bundles + GitHub |
| Secret Management | Databricks Secret Scopes |

## Dataset

**Source:** CMS Medicare Physician & Other Practitioners by Provider and Service  
**URL:** https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners  
**Size:** ~492MB CSV, 9,781,673 rows, 29 columns  
**Update frequency:** Annually  
**Year:** 2024  

## Pipeline Status

| Notebook | Status | Output |
|---|---|---|
| 01_bronze_ingestion | ✅ Complete | 9,781,673 rows → `bronze.provider_utilization_raw` |
| 02_silver_transform | ✅ Complete | 96,367 providers · 832,539 procedures · 22 quarantined → `silver.*` |
| 03_gold_aggregation | 🔄 In Progress | — |

## Unity Catalog Structure

```
cms_medicare_databricks_pipeline
  ├── bronze
  │     └── provider_utilization_raw       ✅  9,781,673 rows
  ├── silver
  │     ├── providers                      ✅  96,367 unique CA providers
  │     ├── procedures                     ✅  832,539 clean rows
  │     └── procedures_quarantine          ✅  22 quarantined rows
  └── gold
        ├── provider_cost_scorecard        ⏳
        └── hospital_readmission_risk      ⏳
```

## Key Data Facts

| Metric | Value |
|---|---|
| National Medicare rows ingested | 9,781,673 |
| California rows after filter | 832,561 (8.51% of national) |
| Unique California providers | 96,367 |
| Clean procedure rows | 832,539 |
| Quarantined rows (invalid payment) | 22 (0.003% — excellent quality) |
| Average procedures per provider | ~8.6 |

## Build Log

**Bronze layer**
- Downloaded 2024 CMS Medicare Provider Utilization dataset (492MB, ~10M rows)
- Uploaded CSV to ADLS Gen2 container `cms-medicare-raw/provider_utilization/`
- Ingested via Auto Loader (cloudFiles) with `trigger(availableNow=True)` to simulate daily batch arrival
- All 29 columns stored as string — no transformations at bronze layer
- Added `ingestion_timestamp` and `source_file` metadata columns for audit trail
- Wrote to Delta table with append-only mode and checkpoint for idempotent re-runs
- Result: 9,781,673 rows in `bronze.provider_utilization_raw`

**Silver layer**
- Read from bronze — never from raw ADLS files again
- Filtered to California providers only (`Rndrng_Prvdr_State_Abrvtn = 'CA'`) — reduced dataset by 91.49%
- Split into two tables by concern: `providers` (who) and `procedures` (what + cost)
- Cast all cost columns from string to `decimal(12,2)` for financial precision — chose decimal over double to avoid floating point rounding errors on dollar amounts
- Cast RUCA codes to `decimal(5,2)` — discovered CMS uses decimal RUCA values (1.1, 2.1 etc.) representing rural-urban gradations
- Deduplicated providers on NPI — 832K procedure rows collapsed to 96,367 unique providers
- Deduplicated procedures on `provider_npi + hcpcs_code + place_of_service` — preserves legitimate facility vs office price differences for same procedure
- Applied data quality checks: null NPI, null HCPCS code, invalid payment amount (≤ 0)
- 22 records with invalid payment amounts routed to `procedures_quarantine` — not dropped
- Used `overwriteSchema=true` and `DROP TABLE IF EXISTS` during development to handle schema evolution
- Result: 96,367 providers · 832,539 clean procedures · 22 quarantined

## Setup & Reproduction

### Prerequisites
- Azure subscription (free tier works)
- Azure Databricks workspace (Premium tier)
- Azure Data Lake Storage Gen2
- Databricks CLI installed locally

### Step 1 — Azure Infrastructure
1. Create Azure Databricks workspace (Premium, Hybrid, East US)
2. Create ADLS Gen2 storage account with hierarchical namespace enabled
3. Create container `cms-medicare-raw`
4. Assign `Storage Blob Data Contributor` role to Unity Catalog Access Connector

### Step 2 — Databricks Configuration
1. Unity Catalog auto-enabled on workspace creation
2. Create schemas: `bronze`, `silver`, `gold` in catalog
3. Set up Databricks secret scope for ADLS authentication:

```bash
databricks secrets create-scope cms-medicare-scope
databricks secrets put-secret cms-medicare-scope adls-storage-key
```

### Step 3 — Data
1. Download CMS Medicare Provider dataset from data.cms.gov (2024)
2. Upload CSV to `cms-medicare-raw/provider_utilization/` in ADLS

### Step 4 — Run Pipeline
Execute notebooks in order:
1. `notebooks/bronze/01_bronze_ingestion.py`
2. `notebooks/silver/02_silver_transform.py`
3. `notebooks/gold/03_gold_aggregation.py` *(in progress)*

## Author

**Davin Kim**  
Databricks Certified Data Engineer Associate  
[LinkedIn](https://www.linkedin.com/in/davinanalytics/) | [GitHub](https://github.com/DavinAnalytics)

---
*Built as Portfolio Project 1 of 2 — Batch Pipeline*  
*Project 2: Real-time Financial Transactions Streaming Pipeline (coming soon)*
