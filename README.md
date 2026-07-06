# CMS Medicare Provider Analytics Pipeline

A production-grade batch data pipeline built on Azure Databricks that ingests, transforms, and aggregates 9.7 million CMS Medicare provider records to analyze healthcare cost variation and hospital readmission risk across California providers.

## Business Questions Answered
- Which California providers have the highest cost variation for common procedures?
- Which California hospitals have the highest readmission risk by condition?

## Architecture

```
Track 1 — Provider Utilization
CMS Provider CSV → bronze.provider_utilization_raw
               → silver.providers + silver.procedures
               → gold.provider_cost_scorecard ✅

Track 2 — Hospital Readmissions
CMS Readmissions CSV → bronze.readmissions_raw
                    → silver.readmissions
                    → gold.hospital_readmission_risk ✅

Both tracks orchestrated in parallel via Lakeflow Jobs
Trigger: File arrival (Azure Event Grid) → job fires when new CSV lands in ADLS
Unity Catalog — Governance, lineage, access control across all layers
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
| Orchestration | Lakeflow Jobs (parallel DAG, file arrival trigger) |
| CI/CD | Databricks Asset Bundles + GitHub |
| Secret Management | Databricks Secret Scopes + Job Cluster Spark Config |
| Performance | Liquid Clustering |
| Event Driven | Azure Event Grid + Storage Queue |

## Datasets

### Track 1 — Provider Utilization
**Source:** CMS Medicare Physician & Other Practitioners by Provider and Service  
**URL:** https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners  
**Size:** ~492MB CSV, 9,781,673 rows, 29 columns  
**Update frequency:** Annually · **Year:** 2024  

### Track 2 — Hospital Readmissions
**Source:** CMS Hospital Readmissions Reduction Program (HRRP)  
**URL:** https://data.cms.gov/provider-data/dataset/9n3s-kdb3  
**Size:** ~1MB CSV, 18,330 rows, 12 columns  
**Update frequency:** Annually · **Year:** FY2026  

## Pipeline Status

| Notebook | Status | Output |
|---|---|---|
| 01_bronze_ingestion | ✅ Complete | 9,781,673 rows → `bronze.provider_utilization_raw` |
| 02_silver_transform | ✅ Complete | 96,367 providers · 832,539 procedures · 22 quarantined |
| 03_gold_aggregation | ✅ Complete | 832,539 rows → `gold.provider_cost_scorecard` |
| 04_bronze_readmissions | ✅ Complete | 18,330 rows → `bronze.readmissions_raw` |
| 05_silver_readmissions | ✅ Complete | 1,662 rows · 277 CA hospitals · 6 conditions |
| 06_gold_readmission_risk | ✅ Complete | 1,662 rows → `gold.hospital_readmission_risk` |
| Lakeflow Jobs orchestration | 🔄 In Progress | Parallel DAG · file arrival trigger |

## Unity Catalog Structure

```
cms_medicare_databricks_pipeline
  ├── bronze
  │     ├── provider_utilization_raw       ✅  9,781,673 rows
  │     └── readmissions_raw               ✅  18,330 rows
  ├── silver
  │     ├── providers                      ✅  96,367 unique CA providers
  │     ├── procedures                     ✅  832,539 clean rows
  │     ├── procedures_quarantine          ✅  22 quarantined rows
  │     └── readmissions                   ✅  1,662 rows · 277 CA hospitals
  └── gold
        ├── provider_cost_scorecard        ✅  832,539 rows · liquid clustered
        └── hospital_readmission_risk      ✅  1,662 rows · liquid clustered
```

## Key Data Facts

| Metric | Value |
|---|---|
| National Medicare provider rows ingested | 9,781,673 |
| California rows after filter | 832,561 (8.51% of national) |
| Unique California providers | 96,367 |
| Clean procedure rows | 832,539 |
| Quarantined rows (invalid payment) | 22 (0.003% quality rate) |
| Average procedures per provider | ~8.6 |
| OC high cost outliers identified | 18,678 |
| Max cost deviation found | +1,473% above state average |
| CA hospitals tracked in HRRP | 277 |
| Conditions tracked | 6 (AMI, CABG, COPD, HF, Hip/Knee, Pneumonia) |
| High risk hospital-condition pairs | 260 |
| Low risk hospital-condition pairs | 258 |
| Average risk hospital-condition pairs | 520 |

## Key Findings — Provider Cost Scorecard

Top Orange County high-cost outliers identified by the pipeline:

| Provider | City | Procedure | OC Payment | State Avg | Deviation |
|---|---|---|---|---|---|
| Mission Ambulatory Surgicenter | Mission Viejo | Bone marrow biopsy | $902.79 | $57.38 | +1,473% |
| Pegasus Surgery Center | Newport Beach | Brain neurostimulator insertion | $19,847.74 | $1,551.04 | +1,179% |
| Main Street Specialty Surgery Center | Orange | Harvest of graft from small bone | $3,768.61 | $341.89 | +1,002% |
| Specialty Surgical Center of Irvine | Irvine | Penile implant insertion | $13,010.24 | $1,836.85 | +608% |

*Cost deviation measured against California IQR benchmark using standardized payment amounts adjusted for geographic cost differences.*

## Key Findings — Hospital Readmission Risk (FY2026)

California hospitals ranked by average excess readmission ratio:

| Condition | CA Avg Excess Ratio | Interpretation | High Risk Hospitals |
|---|---|---|---|
| Pneumonia | 1.0262 | 2.62% above expected | included in 260 total |
| Heart Failure | 1.0171 | 1.71% above expected | 59 high risk |
| Heart Attack (AMI) | 1.0154 | 1.54% above expected | 35 high risk |
| CABG | 1.0052 | Essentially at expected | 22 high risk |
| COPD | 1.0023 | Essentially at expected | 50 high risk |
| Hip & Knee | 0.9788 | 2.12% BETTER than expected ✅ | — |

Top CA hospitals by AMI (Heart Attack) readmission risk:
1. Good Samaritan Hospital
2. Centinela Hospital Medical Center
3. Washington Hospital
4. Valley Presbyterian Hospital
5. Regional Medical Center of San Jose

*Excess readmission ratio > 1.0 = more readmissions than CMS expects = potential Medicare payment penalty.*

## Build Log

**Bronze — provider utilization**
- Downloaded 2024 CMS Medicare Provider Utilization dataset (492MB, ~10M rows)
- Ingested via Auto Loader (cloudFiles) with `trigger(availableNow=True)` to simulate daily batch arrival
- All 29 columns stored as string — no transformations at bronze layer
- Added `ingestion_timestamp` and `source_file` metadata columns for audit trail
- Authentication handled at job cluster level via Spark config secret interpolation — no auth code in notebooks
- Result: 9,781,673 rows in `bronze.provider_utilization_raw`

**Silver — provider utilization**
- Filtered to California providers only — reduced dataset by 91.49%
- Split into two tables: `providers` (who) and `procedures` (what + cost)
- Cast cost columns to `decimal(12,2)` — chose decimal over double to avoid floating point rounding errors
- Cast RUCA codes to `decimal(5,2)` — CMS uses decimal RUCA values representing rural-urban gradations
- Deduplicated on `provider_npi + hcpcs_code + place_of_service` — preserves facility vs office price differences
- Data quality checks routed 22 invalid payment records to quarantine — not dropped
- Result: 96,367 providers · 832,539 clean procedures · 22 quarantined

**Gold — provider cost scorecard**
- Written in Spark SQL to demonstrate SQL proficiency alongside PySpark
- Two-CTE query joining silver.procedures + silver.providers + state-wide benchmarks
- Used `avg_medicare_standardized_amount` for fair geographic comparison across CA regions
- IQR-based cost outlier classification (High/Normal/Low) — more robust than standard deviation for right-skewed healthcare cost data
- Orange County flag using zip code prefixes 926xx, 927xx, 928xx
- Added billing ratio (submitted charge / Medicare payment) to surface aggressive billing patterns
- Applied Liquid Clustering on `(hcpcs_code, region, cost_outlier)` for adaptive query optimization
- Result: 832,539 rows · 18,678 OC high cost outliers · max deviation +1,473% above state average

**Bronze — readmissions**
- Downloaded FY2026 CMS Hospital Readmissions Reduction Program dataset (18,330 rows, 12 columns)
- Column names had spaces — renamed to snake_case at bronze since Delta Lake rejects spaces in column names
- Verified 100% row fidelity: raw CSV (18,330) = bronze table (18,330) — Match: True ✓
- Authentication handled at job cluster level — no auth code in notebook
- Result: 18,330 rows in `bronze.readmissions_raw`

**Silver — readmissions**
- Written entirely in Spark SQL — straightforward enrichment with no complex deduplication logic
- No storage authentication needed — reading from Unity Catalog Delta tables, not raw ADLS paths
- Used `TRY_CAST` for all numeric columns — handles 'N/A', 'Too Few to Report' gracefully → NULL
- Used `TO_DATE(col, 'MM/dd/yyyy')` for dates — CMS uses American date format which CAST AS DATE rejects
- Added `condition_name` mapping CMS codes to human-readable clinical descriptions
- Filtered to California only — 18,330 national rows reduced to 1,662 CA rows
- Result: 1,662 rows · 277 unique CA hospitals · 6 conditions

**Gold — hospital readmission risk**
- Written in Spark SQL — two CTEs with RANK() window function
- Calculated California state-level benchmarks per condition (avg, p25, median, p75)
- IQR classification: High Risk (above p75) / Average Risk / Low Risk (below p25) / Suppressed
- `RANK() OVER (PARTITION BY measure_name ORDER BY excess_readmission_ratio DESC NULLS LAST)`
- LEFT JOIN preserves all hospitals including those with fully suppressed conditions
- Applied Liquid Clustering on `(condition_name, performance_category)`
- Result: 1,662 rows · 260 high risk · 258 low risk · 520 average risk hospital-condition pairs

**Authentication architecture**
- Development (interactive notebooks): `dbutils.secrets.get()` + `spark.conf.set()` in notebook Cell 1
- Production (Lakeflow Jobs): ADLS key injected via job cluster Spark config using secret interpolation syntax `{{secrets/cms-medicare-scope/adls-storage-key}}` — no auth code in notebooks
- Silver and gold notebooks: no authentication needed — Unity Catalog manages access to Delta tables transparently
- External location registered in Unity Catalog with Azure Event Grid integration for file arrival trigger
- IAM roles on Access Connector: Storage Blob Data Contributor, Storage Account Contributor, EventGrid EventSubscription Contributor, Storage Queue Data Contributor

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
4. Assign IAM roles to Unity Catalog Access Connector:
   - `Storage Blob Data Contributor`
   - `Storage Account Contributor`
   - `EventGrid EventSubscription Contributor`
   - `Storage Queue Data Contributor`
5. Register `Microsoft.EventGrid` resource provider in Azure subscription

### Step 2 — Databricks Configuration
1. Unity Catalog auto-enabled on workspace creation
2. Create schemas: `bronze`, `silver`, `gold` in catalog
3. Set up Databricks secret scope for ADLS authentication:

```bash
databricks secrets create-scope cms-medicare-scope
databricks secrets put-secret cms-medicare-scope adls-storage-key
```

4. Register external location in Unity Catalog pointing to ADLS container
5. Configure job cluster Spark config for secret interpolation:
```
fs.azure.account.key.cmsmedicaredatastorage.dfs.core.windows.net {{secrets/cms-medicare-scope/adls-storage-key}}
```

### Step 3 — Data
1. Download CMS Medicare Provider dataset from data.cms.gov (2024)
2. Upload CSV to `cms-medicare-raw/provider_utilization/` in ADLS
3. Download CMS HRRP dataset from data.cms.gov (FY2026)
4. Upload CSV to `cms-medicare-raw/readmissions/` in ADLS

### Step 4 — Run Pipeline
Execute via Lakeflow Jobs or manually in order:
1. `notebooks/bronze/01_bronze_ingestion.py`
2. `notebooks/silver/02_silver_transform.py`
3. `notebooks/gold/03_gold_aggregation.py`
4. `notebooks/bronze/04_bronze_readmissions.py`
5. `notebooks/silver/05_silver_readmissions.py`
6. `notebooks/gold/06_gold_readmission_risk.py`

## Author

**Davin Kim**  
Databricks Certified Data Engineer Associate  
[LinkedIn](https://www.linkedin.com/in/davinanalytics/) | [GitHub](https://github.com/DavinAnalytics)

---
*Built as Portfolio Project 1 of 2 — Batch Pipeline*  
*Project 2: Real-time Financial Transactions Streaming Pipeline (coming soon)*
