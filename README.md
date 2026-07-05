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
| 01_bronze_ingestion | ✅ Complete | 9,781,673 rows → bronze.provider_utilization_raw |
| 02_silver_transform | 🔄 In Progress | — |
| 03_gold_aggregation | ⏳ Pending | — |

## Unity Catalog Structure

```
cms_medicare_databricks_pipeline
  ├── bronze
  │     └── provider_utilization_raw  ✅
  ├── silver
  │     ├── providers                 ⏳
  │     └── procedures                ⏳
  └── gold
        ├── provider_cost_scorecard   ⏳
        └── hospital_readmission_risk ⏳
```

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
2. `notebooks/silver/02_silver_transform.py` *(in progress)*
3. `notebooks/gold/03_gold_aggregation.py` *(pending)*

## Author

**Davin Kim**  
Databricks Certified Data Engineer Associate  
[LinkedIn](https://www.linkedin.com/in/davinanalytics/) | [GitHub](https://github.com/DavinAnalytics)

---
*Built as Portfolio Project 1 of 2 — Batch Pipeline*  
*Project 2: Real-time Financial Transactions Streaming Pipeline (coming soon)*
