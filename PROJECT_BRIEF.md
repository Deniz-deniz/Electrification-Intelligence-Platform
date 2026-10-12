# PROJECT_BRIEF: Electrification Intelligence Platform

> **Source of Truth** for all design, data engineering, quality governance, and application components of the portfolio project.
> Where later task prompts specify table, column, endpoint, or rejection-code names, user prompts are the primary source of truth.

---

## 1. Executive Summary & Project Goal

**GOAL:** An "Electrification Intelligence Platform". Several source systems each keep their own customer records. We unify them into one trusted view (customer + vehicle by VIN), measure data quality, expose metrics and a data catalog through an API, and show an "Opportunity Radar" web app that lists customers worth contacting for an EV transition, plus management and data-health dashboards.

### Corporate Domain Context
The project models the multi-entity operational ecosystem of an automotive distributor group in Turkey representing premium automotive brands:
- **Passenger & Performance Brands:** BMW, MINI, BMW Motorrad, Range Rover, Defender, Discovery, Jaguar
- **Value-Chain Subsidiaries:**
  - **Borusan Oto:** Dealership sales & authorized service network
  - **Borusan Otomotiv Premium:** Long-term fleet leasing, vehicle financing, and insurance
  - **Borusan Next:** Certified pre-owned vehicle appraisal, trade-in, and remarketing

All operational data is **synthetic** and designed to mirror real-world dirty data challenges faced by automotive conglomerates lacking a unified Master Data Management (MDM) backbone.

---

## 2. System Architecture & Lakehouse Mapping

**ARCHITECTURE (medallion, local implementation that maps 1:1 to a Microsoft Fabric Lakehouse):**
- `data/raw`    : source CSVs (already generated)
- `data/bronze` : raw data loaded as-is + load metadata columns, stored as Parquet
- `data/silver` : cleaned, standardized, deduplicated, customer-matched, PII-masked data + rejected records
- `data/gold`   : business metrics and data-quality scores
- `data/meta`   : pipeline run log (`pipeline_runs.parquet`), data catalog (`catalog.json`, `data_catalog.parquet`), metric glossary (`metrics.json`, `metric_glossary.parquet`)

**Stack:** Python 3.10+, pandas, pyarrow, FastAPI, uvicorn, pytest. No database server. No external API calls of any kind. NO LLM and NO API keys anywhere in this project; everything runs offline.

```
                      ┌──────────────────────────────────────────────┐
                      │                 DATA SOURCES                 │
                      │  DMS Sales | Premium Lease | Service | Next  │
                      └──────────────────────┬───────────────────────┘
                                             │ Ingest as-is
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │              BRONZE LAKEHOUSE                │
                      │   Raw Parquet + Ingestion Audit Metadata     │
                      └──────────────────────┬───────────────────────┘
                                             │ Cleanse, Standardize,
                                             │ Resolve Entities, Quarantine
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │              SILVER LAKEHOUSE                │
                      │  dim_customers (Golden), dim_vehicles (VIN)  │
                      │  fact_sales, fact_contracts, fact_service    │
                      │  fact_trade_ins, rejected_records (Quarantine)│
                      └──────────────────────┬───────────────────────┘
                                             │ Business Aggregations,
                                             │ Opportunity Scoring, DQ KPIs
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │               GOLD LAKEHOUSE                 │
                      │  ev_opportunities, ev_share_by_dealer,       │
                      │  service_cost_comparison, trade_in_value_ratio│
                      │  data_quality_scores                         │
                      └──────────────────────┬───────────────────────┘
                                             │
                       ┌─────────────────────┴─────────────────────┐
                       ▼                                           ▼
         ┌───────────────────────────┐               ┌───────────────────────────┐
         │       FASTAPI BACKEND     │               │   OPPORTUNITY RADAR UI    │
         │  /health, /catalog/*,     │               │  Four Tabs:               │
         │  /quality, /pipeline/runs │               │  - Radar                  │
         │  /gold/*, /silver/*       │               │  - Management             │
         │  (100% Offline)           │               │  - Data Health, - Catalog │
         └───────────────────────────┘               └───────────────────────────┘
```

### Microsoft Fabric Lakehouse 1:1 Correspondence
| Local Project Component | Microsoft Fabric Architecture Equivalent |
|---|---|
| `data/raw/*.csv` | **OneLake Landing Zone / ADLS Gen2 Shortcuts** (Raw external partner drops) |
| `data/bronze/*.parquet` | **Fabric Lakehouse Bronze Tables** (Appended with `_ingested_at`, `_source_file`) |
| `data/silver/*.parquet` | **Fabric Lakehouse Silver Tables** (Cleansed Delta Parquet, V-Order optimized) |
| `data/silver/rejected_records` | **Fabric Data Factory / Lakehouse Quarantine Table** (Dead-letter queue) |
| `data/gold/*.parquet` | **Fabric Gold Lakehouse / Direct Lake Semantic Model** (Ready for Power BI) |
| `data/meta/pipeline_runs` | **Fabric Pipeline Monitoring / Azure Monitor Diagnostic Log Table** |
| `data/meta/data_catalog` | **Microsoft Purview Data Map / Fabric Catalog Item Governance** |

---

## 3. Non-Negotiable Core Rules

**NON-NEGOTIABLE RULES:**
1. Every pipeline step writes a row to `data/meta/pipeline_runs.parquet` (`run_id`, `step`, `layer`, `source`, `rows_in`, `rows_out`, `rows_rejected`, `started_at`, `duration_s`, `status`, `error`).
2. Nothing is silently dropped. Invalid records go to a rejected table with a reason code.
3. Business logic lives in small, pure, unit-tested functions.
4. The reference date for all time-based logic is configurable (`config.py`, default `2026-10-09`). Never call `date.today()` inside business logic.
5. API responses and the web UI must never expose raw phone numbers or full names; use masked values (e.g. `"O*** Ş****"`, `"530***4226"`).
6. Each step must be re-runnable (idempotent) and runnable from one command: `python -m pipeline.run_all`.
7. Keep code simple and readable. Add short docstrings that explain WHY, because I must be able to explain every design choice in an interview.

---

## 4. Source Systems & Data Quality Challenges

The raw operational data simulates 4 disparate systems with no master customer key:

| Source System | File Name | Est. Rows | Customer Identifier | Vehicle Identifier | Key Quality Issues Injected |
|---|---|---|---|---|---|
| **DMS Satış (Borusan Oto)** | `dms_satis.csv` | ~2,732 | `musteri_adi`, `telefon` | `sasi_no` | ~1.5% exact duplicate rows, irregular phone formats, whitespace, Turkish character loss, name inversion/spelling noise. |
| **Premium (Finans/Kiralama)** | `premium_sozlesme.csv` | ~1,445 | `musteri_unvani`, `iletisim_tel` | `vin` | ~2% illogical dates (`bitis < baslangic`), missing phone numbers, date formats (`DD.MM.YYYY`, `DD/MM/YYYY`, `YYYY-MM-DD`), name inversion/spelling noise. |
| **Servis İşlemleri** | `servis_kayit.csv` | ~8,690 | `sahip_adi` *(NO phone column)* | `sasi` | Missing customer phone (has NO phone column, only owner name), vehicle VIN casing / missing VINs, workshop codes, name inversion/spelling noise. |
| **Borusan Next (İkinci El)** | `next_islem.csv` | ~362 | `musteri` | `vin_no` | First/last name inversion, spelling noise, appraisal transaction types. |
| **Yetkili Ağ** | `bayi_servis_noktalari.csv` | 18 | N/A | `dealer_code` | Reference dimension table (Bayi vs. Servis). |

*Note on Data Noise:* Name inversion (e.g., `Şahin Ozan` vs. `Ozan Şahin`), case anomalies, extra spaces, and Turkish character loss (`GUSIOCgusioc`) appear across **all** source systems.

---

## 5. Silver Layer: Entity Resolution & Data Governance

### 5.1 Golden Customer Record (Master Data Management)
Customer resolution follows deterministic and conservative record linkage:
1. **Golden Record Matching Rule:**
   > **"Never merge customers on name alone; a name match needs a shared valid VIN or phone."**
   > Token-sorted names may only be used together with either a shared valid VIN or a shared normalized phone number.
2. **Phone Standardization:** Normalize to a canonical 10-digit Turkish mobile format (`5XXXXXXXXX`). Empty or invalid phones are flagged and cannot serve as linkage anchors.
3. **Name Standardization:** Unicode normalization, Turkish uppercase/lowercase mapping (`İ/i`, `I/ı`), whitespace collapsing, and token sorting (`"Ozan Şahin"` $\rightarrow$ `["OZAN", "SAHIN"]`).
4. **Graph/Cluster Linkage:** Vehicles (`vin`) link transactions across sales, contracts, and service history. Where records match on normalized phone or token-sorted name paired with a shared VIN or phone, link to a single persistent `musteri_id`.
5. **Accuracy Benchmarking:** Compare the resolved clusters against `data/ground_truth/vin_to_customer.csv` to calculate verifiable **Precision, Recall, Accuracy, and Cluster Purity** scores.

### 5.2 Quarantine & Rejection Architecture
Rows failing structural or business rules are quarantined in `data/silver/rejected_records.parquet`.

**Standardized Rejection Codes:**
- `INVALID_VIN`: VIN is malformed, not 17 characters, or contains invalid characters (I, O, Q).
- `MISSING_VIN`: VIN field is null, empty, or whitespace.
- `INVALID_DATE`: Date is unparseable or outside permissible domain boundaries.
- `CONTRACT_END_BEFORE_START`: Contract end date is prior to start date (`bitis < baslangic`).
- `DUPLICATE_KEY`: Duplicate primary/transaction key (e.g., repeated `sale_id`).

**Rejected Table Schema (`data/silver/rejected_records.parquet`):**
- `source_table` (string): Originating table / source name (e.g., `dms_satis`, `premium_sozlesme`).
- `source_row_id` (string): Physical row identifier / position in the Bronze table (e.g., `row_0`, `row_1`).
- `source_business_key` (string): Business key from the source record (e.g., `sale_id`, `contract_id`).
- `rule_code` (string): Rejection code (`INVALID_VIN`, `MISSING_VIN`, `INVALID_DATE`, `CONTRACT_END_BEFORE_START`, `DUPLICATE_KEY`).
- `rule_description` (string): Human-readable explanation of why the record was rejected.
- `raw_value` (string): Problematic value or JSON-serialized raw row payload.
- `run_id` (string): UUID of the pipeline run that caught and quarantined the record.

### 5.3 KVKK Compliance & PII Masking
Raw names and phone numbers are isolated strictly inside internal silver transformations. Silver presentation tables, Gold tables, API outputs, and UI displays strictly present masked tokens:
- **Name Masking:** First letter of each word preserved, remainder masked (`"Ozan Şahin"` $\rightarrow$ `"O*** Ş****"`).
- **Phone Masking:** First 3 digits and last 4 digits preserved (`"5305614226"` $\rightarrow$ `"530***4226"`).

---

## 6. Gold Layer & Opportunity Radar Engine

The Gold layer materializes business aggregations that drive decision-making:
1. **`ev_opportunities` (`data/gold/ev_opportunities.parquet`):**
   A ranked list of customer-vehicle pairs targeted for an EV transition based on the Opportunity Radar propensity score ($0 \dots 100$):
   - **Contract Expiry Urgency ($w_1 = 0.35$):** Lease/finance contracts expiring within `RADAR_EXPIRY_HORIZON_DAYS` (180 days from `REFERENCE_DATE`).
   - **Vehicle Age & Lifecycle ($w_2 = 0.25$):** ICE vehicles older than 2.5–3 years.
   - **Maintenance Burden ($w_3 = 0.20$):** High workshop spend or frequent maintenance visits.
   - **Trade-In Engagement ($w_4 = 0.20$):** Active appraisal or valuation request at Borusan Next.
2. **`ev_share_by_dealer` (`data/gold/ev_share_by_dealer.parquet`):**
   Adoption metrics, EV vs. ICE sales volume, and penetration percentage aggregated by dealer and brand (highlighting MINI's ~90% EV mix).
3. **`service_cost_comparison` (`data/gold/service_cost_comparison.parquet`):**
   Comparative maintenance spend between ICE and EV powertrains (includes average service ticket and service cost per kilometer as an extra metric).
4. **`trade_in_value_ratio` (`data/gold/trade_in_value_ratio.parquet`):**
   Borusan Next valuation and residual value retention ratio by vehicle brand and fuel type.
5. **`data_quality_scores` (`data/gold/data_quality_scores.parquet`):**
   Layer-by-layer completeness, non-null domain rule validity rates, uniqueness percentages, and overall DQ governance index.
6. **`referential_integrity_checks` (`data/gold/referential_integrity_checks.parquet`):**
   Foreign key referential integrity audits between Silver fact tables (`service_records`, `contracts`, `trade_ins`) and vehicle dimension / sales lineage, capturing orphan records caused by quarantined sales.
7. **`fuel_type_verification_queue` (`data/gold/fuel_type_verification_queue.parquet`):**
   Operational queue of customers whose only vehicles have unknown fuel type (102 leads) routed for fuel type verification before EV conversion evaluation.

---

## 7. Delivery Interfaces (API & Web UI)

### 7.1 FastAPI Backend Endpoints (`api/`)
- `GET /health` : System readiness, layer status, and last run timestamp.
- `GET /catalog/tables` : List of all tables across Bronze, Silver, Gold, and Meta.
- `GET /catalog/tables/{table_name}` : Detailed schema, column data types, nullability, and descriptions for `{table_name}`.
- `GET /catalog/metrics` : Metric definitions, calculation formulas, and business owners.
- `GET /catalog/search` : Search tables, columns, or metrics by query string.
- `GET /quality` : Comprehensive data quality dashboard data, rejection counts, and validity rates.
- `GET /pipeline/runs` : Audit log of pipeline executions and step-level metrics.
- `GET /gold/*` : Dynamic or dedicated endpoints serving Gold tables (`/gold/ev_opportunities`, `/gold/ev_share_by_dealer`, `/gold/service_cost_comparison`, etc.).
- `GET /silver/*` : Dynamic or dedicated endpoints serving Silver dimensions and facts with masked PII.

### 7.2 Web Application Structure (`app/`)
The web application provides an executive and operational interface organized into **four dedicated tabs**:
1. **Radar:** Filterable and sortable list of customers primed for EV transition (masked customer names, masked phones, current vehicle, fuel type, contract expiry status, and composite opportunity score).
2. **Management:** Macro KPIs on fleet electrification, brand-by-brand EV adoption rates, service cost differences, and trade-in retention ratios.
3. **Data Health:** Data quality cockpit featuring layer-by-layer row counts, rejection distributions by rule code, audit run telemetry, and entity resolution cluster purity score against ground truth.
4. **Catalog:** Interactive data dictionary and metric glossary displaying schemas, lineage, and business definitions.

---

## 8. Interview Talking Points ("Why This Architecture?")

1. **"Why Lakehouse Medallion without a Relational Database?"**
   - Mirrors modern cloud data platforms (OneLake / Delta Lake). Storing analytical data in columnar Parquet provides optimal analytical throughput, zero database server maintenance, and seamless portability to Microsoft Fabric.
2. **"Why a Quarantine Rejection Table instead of silent dropping or forced cleaning?"**
   - In regulatory and enterprise environments (KVKK, financial audits), silent data loss is unacceptable. Quarantining with explicit reason codes enables data quality monitoring and upstream feedback loops to source application teams.
3. **"Why a Conservative Entity Resolution Rule (Never Merge on Name Alone)?"**
   - In Turkey, common name-surname combinations (e.g., Mehmet Demir, Ahmet Kaya) can cause catastrophic false positive merges if linked on name alone. Requiring a shared VIN or verified phone ensures cluster integrity, verified via cluster purity against ground truth.
4. **"Why a Configurable Reference Date (`2026-10-09`)?"**
   - Production ETL pipelines must support deterministic backfills, replayability, and backtesting. Decoupling business logic from `date.today()` makes every function pure and reproducible.
