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
        ↓ Lakeflow Jobs (daily schedule, parallel tracks)
Unity Catalog — Governance, lineage, access control
```

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (Premium, Hybrid) + Serverless |
| Table Format | Delta Lake |
| Ingestion | Auto Loader (cloudFiles) |
| Language | PySpark + Spark SQL |
| Governance | Unity Catalog |
| Orchestration | Lakeflow Jobs (parallel DAG) |
| CI/CD | Databricks Asset Bundles + GitHub |
| Secret Management | Databricks Secret Scopes |
| Performance | Liquid Clustering |

## Dataset

### Track 1 — Provider Utilization
**Source:** CMS Medicare Physician & Other Practitioners by Provider and Service  
**URL:** https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners  
**Size:** ~492MB CSV, 9,781,673 rows, 29 columns  
**Update frequency:** Annually · **Year:** 2024  

### Track 2 — Hospital Readmissions
**Source:** CMS Hospital Readmissions Reduction Program  
**URL:** https://data.cms.gov/provider-data/dataset/9n3s-kdb3  
**Update frequency:** Annually · **Year:** 2024  

## Pipeline Status

| Notebook | Status | Output |
|---|---|---|
| 01_bronze_ingestion | ✅ Complete | 9,781,673 rows → `bronze.provider_utilization_raw` |
| 02_silver_transform | ✅ Complete | 96,367 providers · 832,539 procedures · 22 quarantined |
| 03_gold_aggregation | ✅ Complete | 832,539 rows → `gold.provider_cost_scorecard` |
| 04_bronze_readmissions | ⏳ Pending | — |
| 05_silver_readmissions | ⏳ Pending | — |
| 06_gold_readmission_risk | ⏳ Pending | — |

## Unity Catalog Structure

```
cms_medicare_databricks_pipeline
  ├── bronze
  │     ├── provider_utilization_raw       ✅  9,781,673 rows
  │     └── readmissions_raw               ⏳
  ├── silver
  │     ├── providers                      ✅  96,367 unique CA providers
  │     ├── procedures                     ✅  832,539 clean rows
  │     ├── procedures_quarantine          ✅  22 quarantined rows
  │     └── readmissions                   ⏳
  └── gold
        ├── provider_cost_scorecard        ✅  832,539 rows · liquid clustered
        └── hospital_readmission_risk      ⏳
```

## Key Data Facts

| Metric | Value |
|---|---|
| National Medicare rows ingested | 9,781,673 |
| California rows after filter | 832,561 (8.51% of national) |
| Unique California providers | 96,367 |
| Clean procedure rows | 832,539 |
| Quarantined rows (invalid payment) | 22 (0.003% quality rate) |
| Average procedures per provider | ~8.6 |
| OC high cost outliers identified | 18,678 |
| Max cost deviation found | +1,473% above state average |

## Key Findings — Provider Cost Scorecard

Top Orange County high-cost outliers identified by the pipeline:

| Provider | City | Procedure | OC Payment | State Avg | Deviation |
|---|---|---|---|---|---|
| Mission Ambulatory Surgicenter | Mission Viejo | Bone marrow biopsy | $902.79 | $57.38 | +1,473% |
| Pegasus Surgery Center | Newport Beach | Brain neurostimulator insertion | $19,847.74 | $1,551.04 | +1,179% |
| Main Street Specialty Surgery Center | Orange | Harvest of graft from small bone | $3,768.61 | $341.89 | +1,002% |
| Specialty Surgical Center of Irvine | Irvine | Penile implant insertion | $13,010.24 | $1,836.85 | +608% |

*Data source: CMS Medicare 2024. Cost deviation measured against California IQR benchmark using standardized payment amounts adjusted for geographic cost differences.*

## Build Log

**Bronze layer — provider utilization**
- Downloaded 2024 CMS Medicare Provider Utilization dataset (492MB, ~10M rows)
- Uploaded CSV to ADLS Gen2 container `cms-medicare-raw/provider_utilization/`
- Ingested via Auto Loader (cloudFiles) with `trigger(availableNow=True)` to simulate daily batch arrival
- All 29 columns stored as string — no transformations at bronze layer
- Added `ingestion_timestamp` and `source_file` metadata columns for audit trail
- Wrote to Delta table with append-only mode and checkpoint for idempotent re-runs
- Result: 9,781,673 rows in `bronze.provider_utilization_raw`

**Silver layer — provider utilization**
- Read from bronze — never from raw ADLS files again
- Filtered to California providers only — reduced dataset by 91.49%
- Split into two tables by concern: `providers` (who) and `procedures` (what + cost)
- Cast all cost columns from string to `decimal(12,2)` — chose decimal over double to avoid floating point rounding errors on dollar amounts
- Cast RUCA codes to `decimal(5,2)` — discovered CMS uses decimal RUCA values (1.1, 2.1 etc.) representing rural-urban gradations
- Deduplicated providers on NPI — 832K rows collapsed to 96,367 unique providers
- Deduplicated procedures on `provider_npi + hcpcs_code + place_of_service` — preserves legitimate facility vs office price differences
- Applied data quality checks: null NPI, null HCPCS code, invalid payment (≤ 0)
- 22 records routed to `procedures_quarantine` — not dropped
- Result: 96,367 providers · 832,539 clean procedures · 22 quarantined

**Gold layer — provider cost scorecard**
- Written in Spark SQL to demonstrate SQL proficiency alongside PySpark
- Two-CTE query joining silver.procedures + silver.providers + state-wide benchmarks
- Used `avg_medicare_standardized_amount` for fair geographic comparison across CA regions
- Calculated state-wide p25, median, p75 per procedure as IQR benchmarks — more robust than standard deviation for right-skewed healthcare cost distributions
- Cost outlier classification: High (above p75) / Normal (p25-p75) / Low (below p25)
- Orange County flag using zip code prefixes 926xx, 927xx, 928xx
- Added billing ratio (submitted charge / Medicare payment) to surface aggressive billing patterns
- Applied Liquid Clustering on `(hcpcs_code, region, cost_outlier)` for adaptive query optimization
- Result: 832,539 rows · 18,678 OC high cost outliers · max deviation +1,473% above state average

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
3. Download CMS Hospital Readmissions dataset from data.cms.gov
4. Upload CSV to `cms-medicare-raw/readmissions/` in ADLS

### Step 4 — Run Pipeline
Execute notebooks in order:
1. `notebooks/bronze/01_bronze_ingestion.py`
2. `notebooks/silver/02_silver_transform.py`
3. `notebooks/gold/03_gold_aggregation.py`
4. `notebooks/bronze/04_bronze_readmissions.py` *(pending)*
5. `notebooks/silver/05_silver_readmissions.py` *(pending)*
6. `notebooks/gold/06_gold_readmission_risk.py` *(pending)*

## Author

**Davin Kim**  
Databricks Certified Data Engineer Associate  
[LinkedIn](https://www.linkedin.com/in/davinanalytics/) | [GitHub](https://github.com/DavinAnalytics)

---
*Built as Portfolio Project 1 of 2 — Batch Pipeline*  
*Project 2: Real-time Financial Transactions Streaming Pipeline (coming soon)*
