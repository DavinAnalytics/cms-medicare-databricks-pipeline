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
Trigger: File arrival (Azure Event Grid, UC-managed file events) → job fires when new CSV lands in ADLS
Unity Catalog — Governance, lineage, access control across all layers, all compute types
Compute: 100% Serverless — no classic/dedicated clusters in the deployed pipeline
```

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Compute | Azure Databricks (Premium) — **Serverless only** |
| Table Format | Delta Lake |
| Ingestion | Auto Loader (cloudFiles), `Trigger.AvailableNow()` |
| Language | PySpark + Spark SQL |
| Governance | Unity Catalog — storage credentials + external locations for all ADLS access |
| Orchestration | Lakeflow Jobs (parallel DAG, file arrival trigger via UC-managed file events) |
| CI/CD | Databricks Asset Bundles + GitHub |
| Cloud Auth | Azure Managed Identity via Access Connector for Azure Databricks — no storage account keys |
| Performance | Liquid Clustering |
| Event Driven | Azure Event Grid + Storage Queue, managed through UC external location file events |

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

| Notebook | Status | Compute | Output |
|---|---|---|---|
| 01_bronze_ingestion | ✅ Complete | Serverless | 9,781,673 rows → `bronze.provider_utilization_raw` |
| 02_silver_transform | ✅ Complete | Serverless | 96,367 providers · 832,539 procedures · 22 quarantined |
| 03_gold_aggregation | ✅ Complete | Serverless | 832,539 rows → `gold.provider_cost_scorecard` |
| 04_bronze_readmissions | ✅ Complete | Serverless | 18,330 rows → `bronze.readmissions_raw` |
| 05_silver_readmissions | ✅ Complete | Serverless | 1,662 rows · 277 CA hospitals · 6 conditions |
| 06_gold_readmission_risk | ✅ Complete | Serverless | 1,662 rows → `gold.hospital_readmission_risk` |
| Lakeflow Jobs orchestration | In progress | Serverless | Two independent jobs, each with its own file arrival trigger |

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

## Authentication & Governance Architecture (current)

All ADLS access — bronze ingestion included — is governed entirely through **Unity Catalog external locations**, backed by an **Azure Managed Identity** via an **Access Connector for Azure Databricks**. There is no storage account key, secret scope, or `spark.conf`-based authentication anywhere in this pipeline.

- **Storage credential**: `cms_medicare_raw_credential` — an Azure Managed Identity, connected through `unity-catalog-access-connector`.
- **External location**: `cms-medicare-data-storage`, scoped to `abfss://cms-medicare-raw@cmsmedicaredatastorage.dfs.core.windows.net/`, with **file events enabled** for file arrival triggers.
- **IAM roles granted to the Access Connector's managed identity:**
  - `Storage Blob Data Contributor` — on the storage account (read/write data)
  - `Storage Queue Data Contributor` — on the storage account (file event queue operations)
  - `EventGrid Data Contributor` — on the storage account (managed Event Grid subscription setup)
  - `EventGrid EventSubscription Contributor` — on the resource group
  - `Microsoft.EventGrid` resource provider registered on the Azure subscription
- **Compute**: every notebook — bronze, silver, and gold, for both tracks — runs on **Serverless compute**. Auto Loader in bronze uses `Trigger.AvailableNow()`, which processes everything available since the last checkpoint and exits; this is the exact triggered/incremental streaming pattern Serverless is designed for, so there is no technical reason to keep a warm classic cluster running between file arrivals.
- Bronze, silver, and gold all authenticate identically — UC vends short-lived credentials automatically the moment code touches a governed `abfss://` path or a UC-managed Delta table. No notebook contains any authentication code.

## What Went Wrong: A Credential Conflict, Not a Data Problem

While building each medallion layer individually, bronze ingestion ran using a **storage account key** injected into the job cluster's Spark config via a Databricks secret scope (`fs.azure.account.key.<storage>.dfs.core.windows.net {{secrets/...}}`). This is the legacy, pre-Unity-Catalog way of authenticating to ADLS, and at that point in development it worked — each notebook ran in isolation, and nothing was yet exercising the code path where Unity Catalog's own credential system got involved.

Once I moved from individually testing notebooks to testing orchestration end-to-end, bronze ingestion started throwing:

```
PERMISSION_DENIED: The credential 'cms_medicare_databricks_pipeline' is a workspace
default credential that is only allowed to access data in the following paths:
'abfss://unity-catalog-storage@dbstorageu25jgq5vd3tok.dfs.core.windows.net/...'
```

**Root cause:** on Unity-Catalog-enabled compute, every `abfss://` filesystem call is intercepted by UC and resolved through UC's own credential system first — before any legacy `spark.conf` key setting is ever consulted. This is intentional platform behavior: it stops a raw storage key from being used to route around UC's governance, access control, and audit trail. Once orchestration testing exercised a code path where this interception actually fired, UC looked for a governing external location for the raw data path, found none correctly wired to it, and fell back to the catalog's auto-generated default managed-storage credential — which is scoped only to Databricks' own internal storage container, not my ADLS account. The key hadn't broken or expired; it had simply stopped being consulted at all, silently, the moment the right conditions were met.

**The actual mistake:** treating storage-account-key auth and Unity Catalog governance as compatible when they're not — the whole point of external locations is to *replace* the secret-scope-key pattern, not sit alongside it. Silver and gold never hit this because they only ever touched UC-managed Delta tables, never raw ADLS paths directly — which is also why the bug stayed invisible until orchestration specifically exercised bronze's raw storage read.

## How I Debugged It

1. Read the full stack trace rather than just the top-line error — the credential name in the error (`cms_medicare_databricks_pipeline`) matched my catalog name exactly, which was the tell that it was the catalog's auto-generated default credential, not a real storage credential I'd created.
2. Ran `SHOW STORAGE CREDENTIALS` and `SHOW EXTERNAL LOCATIONS` to see what UC objects actually existed, rather than assuming.
3. Ran `DESCRIBE EXTERNAL LOCATION` on the external location backing my raw data path and confirmed its `credential_name` pointed at the wrong (default) credential — the external location object existed, but was wired to the wrong authorization source.
4. Created a dedicated storage credential (`cms_medicare_raw_credential`) backed by my own Access Connector, and repointed the external location at it.
5. Hit a secondary error trying to swap credentials via the UI (`Cannot update external location... because the external location has dependent managed file event queue`) — resolved by using the Databricks SDK's `force=True` option (`w.external_locations.update(..., force=True)`), which isn't exposed in the Catalog Explorer UI but is available via the API/SDK.
6. Verified the fix with **Test Connection** in Catalog Explorer (Read/List/Write/Delete/Path Exists/Hierarchical Namespace/File Events Read — all green) before touching the notebook again.
7. Removed the leftover `spark.conf` key setting from the cluster's Spark config entirely, confirmed **Dedicated access mode**, cleared notebook state/outputs and stale checkpoint paths, and re-ran bronze end-to-end.
8. Once bronze was confirmed working, tested whether it actually needed Dedicated compute at all — built an isolated test (throwaway checkpoint + throwaway table, same real source path) on **Serverless**, confirmed a full 9.78M-row read/write succeeded, then migrated both bronze notebooks to Serverless permanently.

## What I Learned

- **Legacy auth patterns and Unity Catalog can silently conflict, and UC wins.** If a pipeline is meant to be UC-governed, every storage path needs a proper external location from the start — a storage key sitting alongside UC is a latent bug, not a fallback.
- **A bug can be invisible until a different execution path exercises it.** Silver and gold looked "done" for the entire time bronze was broken, because they never touched raw ADLS at all. Testing individual notebooks in isolation isn't the same as testing the orchestrated system.
- **Read the exact object name in a permission error.** The credential name matching my catalog name was the single clue that made the root cause obvious instead of a long guessing exercise.
- **UI dead ends usually have an API escape hatch.** The `force` option for updating a credential with a dependent file event queue exists on the SDK/API but not in Catalog Explorer's UI — worth checking the CLI/SDK reference whenever a UI action refuses with a "use force" message it doesn't actually let you invoke.
- **Compute choice should follow workload shape, not habit.** Once bronze was fixed, the deeper question was whether it needed a persistent cluster at all — it didn't, because `Trigger.AvailableNow()` combined with a file-arrival trigger is a triggered/incremental pattern, not a continuously-polling one, and that's exactly what Serverless is built for. Matching compute type to trigger semantics, not just "streaming = needs a cluster," is what actually eliminated the idle-cost problem.

## Setup & Reproduction

### Prerequisites
- Azure subscription (free tier works)
- Azure Databricks workspace (Premium tier)
- Azure Data Lake Storage Gen2, hierarchical namespace enabled
- Databricks CLI installed locally

### Step 1 — Azure Infrastructure
1. Create Azure Databricks workspace (Premium tier).
2. Create ADLS Gen2 storage account with hierarchical namespace enabled; create container `cms-medicare-raw`.
3. Create an **Access Connector for Azure Databricks** (system-assigned managed identity).
4. Assign the connector's managed identity these IAM roles:
   - `Storage Blob Data Contributor` — on the storage account
   - `Storage Queue Data Contributor` — on the storage account
   - `EventGrid Data Contributor` — on the storage account
   - `EventGrid EventSubscription Contributor` — on the resource group
5. Register the `Microsoft.EventGrid` resource provider on the Azure subscription.

### Step 2 — Unity Catalog Configuration
1. Confirm the workspace is Unity Catalog enabled (auto-enabled on workspace creation in current Databricks).
2. Create schemas: `bronze`, `silver`, `gold` in the catalog.
3. Create a **storage credential** (Catalog → External Data → Credentials) of type Azure Managed Identity, referencing the Access Connector's resource ID from Step 1.
4. Create an **external location** pointing at `abfss://cms-medicare-raw@<storage-account>.dfs.core.windows.net/`, using that storage credential, with **file events enabled** (Automatic mode).
5. Grant `READ FILES` / `WRITE FILES` on the external location to the identities that need it.
6. No secret scopes, no storage keys, no `spark.conf` authentication anywhere.

### Step 3 — Data
1. Download the CMS Medicare Provider dataset from data.cms.gov (2024).
2. Upload the CSV to `cms-medicare-raw/provider_utilization/` in ADLS.
3. Download the CMS HRRP dataset from data.cms.gov (FY2026).
4. Upload the CSV to `cms-medicare-raw/readmissions/` in ADLS.

### Step 4 — Deploy and Run
```bash
databricks bundle validate
databricks bundle deploy -t dev
```
Both jobs (`provider_utilization_pipeline`, `hospital_readmissions_pipeline`) are defined via Databricks Asset Bundles in `resources/*.yml`, each running entirely on Serverless compute with its own `file_arrival` trigger against the external location. Dropping a new file into either source subpath fires the corresponding job automatically — bronze → silver → gold, end to end.

## Author

**Davin Kim**
Databricks Certified Data Engineer Associate
[LinkedIn](https://www.linkedin.com/in/davinanalytics/) | [GitHub](https://github.com/DavinAnalytics)

---
*Built as Portfolio Project 1 of 2 — Batch Pipeline*
*Project 2: Real-time Financial Transactions Streaming Pipeline (coming soon)*