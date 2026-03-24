# Databricksint — Customer ETL Pipeline

A simple end-to-end ETL pipeline built on **Databricks** that ingests customer data through a
Bronze → Silver medallion architecture using AutoLoader and Delta tables.

---

## Repository Structure

```
.
├── data/
│   └── customers.csv               # 20 sample customer records (includes 1 duplicate)
└── notebooks/
    ├── 01_generate_customer_data.py  # Write customer CSV to DBFS landing zone
    ├── 02_bronze_autoloader.py       # Ingest into Bronze using AutoLoader (streaming)
    ├── 03_silver_standard.py         # Clean & dedup → Silver (standard PySpark batch)
    └── 04_silver_dlt_pipeline.py     # Clean & dedup → Silver (Delta Live Tables / DLT)
```

---

## Pipeline Overview

```
Landing Zone (DBFS CSV)
        │
        ▼  AutoLoader (cloudFiles)
┌───────────────────┐
│  Bronze Schema    │  etl_demo.bronze.customers_raw
│  (raw + metadata) │
└───────────────────┘
        │
        ├──────────────────────────────────────┐
        │  Approach A: Standard PySpark         │  Approach B: Delta Live Tables (DLT)
        ▼                                       ▼
┌───────────────────┐               ┌──────────────────────────┐
│  Silver Schema    │               │  Silver DLT Pipeline     │
│  (clean + dedup)  │               │  (declarative, managed)  │
│  etl_demo.silver  │               │  etl_demo.silver_dlt     │
└───────────────────┘               └──────────────────────────┘
```

---

## Customer Data

The `data/customers.csv` file contains **20 records** (19 unique customers + 1 intentional
duplicate of `customer_id = 1002` to demonstrate the deduplication step).

| Column            | Type    | Description                              |
|-------------------|---------|------------------------------------------|
| customer_id       | integer | Unique customer identifier               |
| first_name        | string  | Customer first name                      |
| last_name         | string  | Customer last name                       |
| email             | string  | Email address                            |
| phone             | string  | Phone number                             |
| address           | string  | Street address                           |
| city              | string  | City                                     |
| state             | string  | State / province (2-letter code)         |
| zip_code          | string  | ZIP / postal code                        |
| country           | string  | Country code                             |
| age               | integer | Customer age                             |
| gender            | string  | M / F                                    |
| signup_date       | date    | Account creation date (yyyy-MM-dd)       |
| loyalty_tier      | string  | Bronze / Silver / Gold / Platinum        |
| total_purchases   | double  | Cumulative spend in USD                  |

---

## Notebooks

### 01 — Generate Customer Data
**File:** `notebooks/01_generate_customer_data.py`

Writes the 20-record CSV to the DBFS landing zone at `dbfs:/etl_demo/landing/customers/`.
Run this notebook **once** before running the AutoLoader notebook.

### 02 — Bronze Layer (AutoLoader)
**File:** `notebooks/02_bronze_autoloader.py`

Uses Databricks **AutoLoader** (`cloudFiles` format) to read CSV files from the landing zone
and stream them into the bronze Delta table `etl_demo.bronze.customers_raw`.

Key features:
- Incremental file detection via AutoLoader's cloud-native file notification
- Schema inference and evolution (`cloudFiles.inferColumnTypes`)
- Audit columns added: `_ingest_timestamp`, `_source_file`
- `trigger(availableNow=True)` — processes all pending files then terminates

### 03 — Silver Layer (Standard PySpark Approach)
**File:** `notebooks/03_silver_standard.py`

Reads from the bronze table, applies transformations, and writes to
`etl_demo.silver.customers_clean` using a standard PySpark batch write.

Transformations:
1. Drop rows missing `customer_id`, `email`, `first_name`, or `last_name`
2. Trim whitespace; apply `initcap` to names, `lower` to email, `upper` to state/country
3. Cast `customer_id` → integer, `age` → integer, `total_purchases` → double, `signup_date` → date
4. Filter out records with invalid `age` (≤ 0 or ≥ 120) or negative `total_purchases`
5. **Deduplicate** by `customer_id` — keep the latest record per `_ingest_timestamp`

### 04 — Silver Layer (Delta Live Tables / DLT Approach)
**File:** `notebooks/04_silver_dlt_pipeline.py`

Implements the same bronze-to-silver logic as Notebook 03 but as a **declarative DLT pipeline**.

DLT features used:
- `@dlt.table` — declares managed Delta tables
- `@dlt.expect_or_drop` — drops rows violating data quality rules and records metrics
- `@dlt.view` — declares a live view for downstream aggregations
- `dlt.read_stream()` — incremental reads from upstream live tables

**Deployment steps:**
1. In Databricks Workspace, go to **Workflows → Delta Live Tables → Create Pipeline**
2. Set **Source code** to the path of `04_silver_dlt_pipeline.py`
3. Set **Target schema** to `etl_demo.silver_dlt`
4. Select **Triggered** pipeline mode
5. Click **Start** — DLT handles orchestration, lineage and monitoring automatically

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Databricks Runtime | 12.2 LTS or later (13.x+ recommended for Unity Catalog) |
| Unity Catalog | Required for `etl_demo` catalog/schema management |
| Delta Live Tables | Required for Notebook 04 (included in Databricks Premium / Enterprise) |
| Cluster | Single-node or multi-node cluster with Delta Lake enabled |

---

## Running the Pipeline

### Standard Approach (Notebooks 01–03)

Run the notebooks **in order** on any Databricks cluster:

```bash
01_generate_customer_data.py   # Write CSV to landing zone
02_bronze_autoloader.py        # Ingest into bronze (AutoLoader)
03_silver_standard.py          # Clean, dedup, write to silver
```

### DLT Approach (Notebook 04)

1. First run notebooks **01** and **02** to populate the bronze table.
2. Create a DLT pipeline pointing to `04_silver_dlt_pipeline.py` as described above.
3. The DLT pipeline will read from `etl_demo.bronze.customers_raw` and write clean data to
   `etl_demo.silver_dlt.customers_clean`.

---

## Data Quality Summary

| Check | Action |
|-------|--------|
| Null `customer_id` | Drop row |
| Null or invalid `email` (no `@`) | Drop row |
| Age ≤ 0 or ≥ 120 | Drop row |
| Negative `total_purchases` | Drop row |
| Duplicate `customer_id` | Keep latest by `_ingest_timestamp` |
