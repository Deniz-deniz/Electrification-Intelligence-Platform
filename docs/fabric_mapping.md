# Microsoft Fabric Architecture Mapping & Enterprise Migration Guide

> **Project:** Electrification Intelligence Platform (Borusan Otomotiv Group)  
> **Concept:** End-to-end working prototype on synthetic data  
> **Document Purpose:** Maps every local component of the medallion lakehouse architecture 1:1 to Microsoft Fabric equivalents, transparently details enterprise migration differences, and defines the platform evolution roadmap.  
> **Honesty Note:** Local implementation; maps to Fabric, not tested on Fabric.

---

## 1. Executive Summary & Component Mapping Table

The Electrification Intelligence Platform is engineered following the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**. Its local implementation (Python 3.10+, pandas, pyarrow, columnar Parquet, FastAPI) was intentionally designed to mirror a modern cloud-native **Microsoft Fabric Lakehouse** without introducing heavy cloud dependencies during the prototype phase.

### Component-by-Component Mapping

| Local Component | Local File / Path | Microsoft Fabric Architecture Equivalent | Fabric Workload / Engine |
|---|---|---|---|
| **Raw Landing Zone** | `data/raw/*.csv` | **OneLake Landing Zone / ADLS Gen2 Shortcuts** | OneLake Files / External Shortcuts |
| **Bronze Layer** | `data/bronze/*.parquet` | **Fabric Lakehouse Bronze Tables** (Delta Parquet) | Apache Spark / Delta Lake Engine |
| **Silver Clean Facts** | `data/silver/sales.parquet`<br>`data/silver/contracts.parquet`<br>`data/silver/service_records.parquet`<br>`data/silver/trade_ins.parquet` | **Fabric Lakehouse Silver Tables** (Cleansed Delta Parquet with V-Order) | Apache Spark / Fabric Delta Tables |
| **Silver Golden Dimensions** | `data/silver/customers.parquet`<br>`data/silver/vehicles.parquet`<br>`data/silver/dealers.parquet` | **Fabric Lakehouse Silver Dimensions** (Master Data / MDM Gold Entities) | Spark / Python / Delta Merge |
| **Silver Quarantine Table** | `data/silver/rejected_records.parquet` | **Fabric Quarantine / Dead-Letter Delta Table** | Delta Lake Dead-Letter Queue |
| **Gold Business Marts** | `data/gold/ev_opportunities.parquet`<br>`data/gold/ev_share_by_dealer.parquet`<br>`data/gold/service_cost_comparison.parquet`<br>`data/gold/trade_in_value_ratio.parquet`<br>`data/gold/fuel_type_verification_queue.parquet` | **Fabric Lakehouse Gold Tables & Direct Lake Semantic Model** | Fabric Gold Lakehouse / Direct Lake Power BI |
| **Gold Data Governance** | `data/gold/data_quality_scores.parquet`<br>`data/gold/referential_integrity_checks.parquet` | **Fabric Data Quality / Governance Telemetry Tables** | Fabric Metrics & Governance |
| **Pipeline Step Modules** | `pipeline/bronze.py`<br>`pipeline/silver_clean.py`<br>`pipeline/silver_customers.py`<br>`pipeline/gold.py`<br>`pipeline/catalog.py` | **Fabric Notebooks** (`nb_bronze`, `nb_silver_clean`, `nb_silver_customers`, `nb_gold`, `nb_catalog`) | Fabric Spark Compute Pool (Python/PySpark) |
| **Pipeline Orchestrator** | `pipeline/run_all.py` | **Fabric Data Factory Pipeline** (`pl_electrification_e2e`) | Fabric Data Pipeline Orchestrator |
| **Audit Run Log** | `data/meta/pipeline_runs.parquet` | **Fabric Diagnostic Log / Pipeline Run History Table** | Log Analytics / Lakehouse System Table |
| **Data Catalog & Metrics** | `data/meta/catalog.json`<br>`data/meta/metrics.json`<br>`data/meta/matching_quality.json` | **Microsoft Purview Data Map & Fabric Catalog Item Governance** | Microsoft Purview Governance Portal |
| **Delivery API Tier** | `api/main.py`<br>`api/utils.py` | **Azure Container Apps / Azure App Service / API Management** | Azure Serverless Container / APIM |
| **Executive Front-End** | `app/static/index.html`<br>`app/static/app.js`<br>`app/static/style.css` | **Azure Static Web Apps / Power BI Embedded Report** | Azure Static Web Apps / Power BI Service |

---

## 2. End-to-End Architectural Comparison

```
LOCAL IMPLEMENTATION (Current Offline Prototype)
=============================================================================
  [Raw CSVs] ──> [Bronze Parquet] ──> [Silver Clean & MDM] ──> [Gold Parquet]
                        │                    │                        │
                        ▼                    ▼                        ▼
                 pipeline_runs        rejected_records       Direct Lake CSV Export
                        │                    │                        │
                        └────────────┬───────┘                        ▼
                                     ▼                           Power BI / UI
                               FastAPI Backend ──> Opportunity Radar SPA (HTML/JS)


MICROSOFT FABRIC TARGET ARCHITECTURE (Production Cloud Target)
=============================================================================
  [ADLS Gen2 / DMS] ──OneLake Shortcut──> [OneLake Landing Zone]
                                                 │
                                                 ▼ (pl_electrification_e2e)
                                          [nb_bronze Notebook]
                                                 │
                                                 ▼
                                     [Bronze Lakehouse Delta]
                                                 │
                                                 ▼ (nb_silver_clean & customers)
                                     [Silver Lakehouse Delta] ──> [rejected_records Delta]
                                                 │
                                                 ▼ (nb_gold)
                                     [Gold Lakehouse Delta (V-Order)]
                                                 │
                    ┌────────────────────────────┴───────────────────────────┐
                    ▼                                                        ▼
         [Fabric Direct Lake]                                    [Azure Container Apps]
         [Power BI Semantic Model]                               [FastAPI Headless REST API]
                    │                                                        │
                    ▼                                                        ▼
   [Power BI Executive Dashboards]                        [Borusan Dealer Portal / Web UI]
```

---

## 3. Honest Migration Analysis: What Changes vs. What Stays

When migrating this local codebase into a live Microsoft Fabric tenant, technical requirements shift in infrastructure and operations, while core engineering algorithms remain identical.

### 3.1 What Would NOT Change (Preserved Assets)

1. **Pure Business Logic & Scoring Algorithms:**
   - **Entity Resolution Rules:** The deterministic graph-linkage matching logic (VIN strong link, normalized phone + ASCII name token sort, strict *"never merge on name alone"* policy) remains 100% identical. In Fabric, the Python logic runs directly inside a Spark notebook or via Pandas API on Spark.
   - **Opportunity Radar Propensity Score:** The additive 0–100 scoring model (contract urgency weights, vehicle age thresholds, service spend burden, EV availability flags) remains unchanged.
   - **Data Quality Definitions:** The exact formulas for completeness, rule-based domain validity, and key uniqueness operate identically.
   - **KVKK Privacy Masking Rules:** Name masking (`O*** Ş****`) and phone masking (`530***4226`) remain mandatory at the Silver boundary.
2. **Deterministic Time-Travel Reference Date Architecture:**
   - Decoupling time calculations from `datetime.today()` via configurable reference timestamps (`config.REFERENCE_DATE`) ensures reproducible backfills and idempotent test execution in Fabric.
3. **Comprehensive Pytest Suite:**
   - All 59 unit and integration tests (testing entity resolution edge cases, quarantine routing, scoring boundaries, and privacy compliance) remain the primary quality gate in the CI/CD pipeline (Azure DevOps / GitHub Actions) before publishing code artifacts to Fabric.

---

### 3.2 What WOULD Change (Enterprise Production Requirements)

| Capability Dimension | Local Prototype Implementation | Microsoft Fabric Enterprise Implementation |
|---|---|---|
| **1. Authentication & Security** | No authentication; endpoints and files accessible locally without credentials. | **Microsoft Entra ID (Azure AD)** OAuth2 tokens, Managed Identities for notebooks/pipelines, and **Row-Level Security (RLS)** in Power BI / SQL endpoint ensuring dealers only see their own customer leads. |
| **2. Storage Format & ACID Guarantees** | Plain Parquet files (`.parquet`) written via `pyarrow`. Overwrites replace full files. | **Delta Lake format (`delta-rs` / Delta-Spark)** with ACID transactions, schema enforcement, time-travel history (`VERSION AS OF`), automatic file compaction, and **Microsoft V-Order optimization** for fast analytical scans. |
| **3. Orchestration & Scheduling** | Synchronous Python script (`python -m pipeline.run_all`) executed manually on demand. | **Fabric Data Factory Pipelines (`pl_electrification_e2e`)** featuring cron schedules, tumbling window triggers, automated retry policies, parallel notebook execution branches, and Microsoft Teams / email alert webhooks upon failure. |
| **4. Incremental Data Processing (CDC)** | Full batch snapshot reload across all sources every pipeline execution. | **Incremental Loading via Delta Change Data Feed (CDF)** and watermarking on `_ingested_at`. New sales and service records are merged via Delta `MERGE INTO` rather than rewriting historical tables. |
| **5. Configuration & Secrets Management** | Plain-text `config.py` and local path variables. | **Azure Key Vault** integration. Database connection strings, API tokens, and tenant IDs are retrieved dynamically at runtime using `mssparkutils.credentials.getSecret()`. |
| **6. Capacity & Cost Governance** | Local machine resources; zero ongoing infrastructure or cloud license cost. | **Microsoft Fabric Capacity Units (CU)** billed under an allocated Capacity Reservation (e.g., F64 SKU). Workloads must be optimized for compute consumption, background smoothing, and OneLake storage tiering. |
| **7. API Deployment & Serving** | Uvicorn running locally on `localhost:8000`. | **Azure Container Apps** or **Azure App Service** behind an **Azure API Management (APIM)** gateway with rate limiting, corporate IP filtering, and Entra ID JWT validation. |

---

## 4. Microsoft Purview Integration & Data Governance

In the local implementation, metadata is cataloged in `data/meta/catalog.json`, `data/meta/metrics.json`, and `data/meta/matching_quality.json`. In Microsoft Fabric, this maps directly to **Microsoft Purview**:

1. **OneLake Catalog Item Governance:**
   - Every lakehouse table is automatically discovered, classified, and indexed in the Purview Data Map.
   - Upstream lineage recorded locally (`upstream_tables` in `catalog.json`) translates to automated end-to-end lineage graphs in Purview, visually tracing data flow from raw DMS files through Silver facts to Gold metrics.
2. **Business Glossary & Metric Ownership:**
   - The 13 metrics defined in `metrics.json` (such as `opportunity_score`, `overall_dq_score`, and `rejection_rate`) map directly to **Purview Business Glossary Terms** with assigned business domain owners (Sales Data Team, Aftersales Data Team, Data Governance).
3. **Automated Sensitive Data Classification:**
   - Where local rules enforce manual PII masking, Purview Data Loss Prevention (DLP) policies automatically scan OneLake files for Turkish National ID (TCKN), phone numbers, and payment details, verifying zero raw PII leakage.

---

## 5. Scope Boundary: Natural Language Querying

> [!NOTE]
> **Prototype Scope Boundary:**
> Natural-language querying over the semantic model (e.g., *Microsoft Fabric Copilot*, Azure OpenAI GPT-4o integration, or Power BI Q&A natural-language-to-DAX translation) is **explicitly out of scope** for this prototype.

### Roadmap Phase 2: Conversational Semantic Layer
While this prototype delivers a deterministic, rule-based analytics engine with zero external API dependencies, the enterprise migration roadmap includes conversational capabilities:
- **Phase 2.1 (Copilot for Microsoft Fabric):** Enable Copilot on the Gold Direct Lake semantic model, allowing dealership managers to ask ad-hoc questions in natural Turkish (e.g., *"Hangi bayide sözleşmesi 60 gün içinde bitecek en yüksek puanlı BMW müşterileri var?"*).
- **Phase 2.2 (RAG & LangChain / Semantic Kernel):** Connect Azure OpenAI Service to the Purview Data Catalog and metric glossary, grounding conversational responses in documented formulas.
- **Phase 2.3 (Proactive Teams Bot):** Automate proactive push notifications to sales advisors when high-priority opportunities ($80+$ score) reach their 90-day renewal threshold.
