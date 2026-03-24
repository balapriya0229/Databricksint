# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4: Silver Layer — Clean & Deduplicate (Spark Declarative Pipeline / DLT)
# MAGIC
# MAGIC This notebook uses **Delta Live Tables (DLT)** — Databricks' declarative pipeline framework —
# MAGIC to perform the same bronze-to-silver transformation as Notebook 3, but defined as a
# MAGIC managed DLT pipeline.
# MAGIC
# MAGIC **Key differences from the standard approach:**
# MAGIC - Tables are declared with `@dlt.table` decorators
# MAGIC - Data quality constraints are expressed as `@dlt.expect` / `@dlt.expect_or_drop` rules
# MAGIC - DLT handles orchestration, lineage, monitoring and incremental processing automatically
# MAGIC - The pipeline is deployed and run via the Databricks **Pipelines** UI or REST API
# MAGIC
# MAGIC **How to deploy:**
# MAGIC 1. In Databricks, go to **Workflows → Delta Live Tables → Create Pipeline**
# MAGIC 2. Set **Source code** to this notebook path
# MAGIC 3. Set **Target schema** to `etl_demo.silver_dlt`
# MAGIC 4. Choose **Triggered** or **Continuous** mode and click **Start**
# MAGIC
# MAGIC **Pipeline:** Bronze (`etl_demo.bronze.customers_raw`) → Silver DLT (`customers_clean`)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration — referenced inside DLT table definitions
BRONZE_CATALOG = "etl_demo"
BRONZE_SCHEMA  = "bronze"
BRONZE_TABLE   = "customers_raw"

# ---------------------------------------------------------------------------
# Reusable DLT constraint expressions (single source of truth)
# ---------------------------------------------------------------------------
CONSTRAINTS = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email":       "email IS NOT NULL AND email LIKE '%@%'",
    "valid_age":         "age > 0 AND age < 120",
    "non_negative_spend": "total_purchases >= 0",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Bronze Source (Live Table reference)
# MAGIC
# MAGIC DLT pipelines read upstream data using `dlt.read()` or `spark.table()`.
# MAGIC Here we expose the existing bronze Delta table as a streaming source.

# COMMAND ----------

@dlt.table(
    name="bronze_customers_raw",
    comment="Raw customer records streamed from the bronze Delta table.",
    table_properties={"quality": "bronze"},
)
def bronze_customers_raw():
    return (
        spark.readStream
            .format("delta")
            .table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Quarantine table for records that fail quality checks
# MAGIC
# MAGIC Records that violate any constraint are written here for investigation.
# MAGIC The filter expression is derived from the shared CONSTRAINTS dict, so
# MAGIC any change to the validation rules is reflected in both tables automatically.

# COMMAND ----------

@dlt.table(
    name="silver_customers_quarantine",
    comment="Customer records that failed data-quality validation.",
    table_properties={"quality": "quarantine"},
)
def silver_customers_quarantine():
    # A row is invalid if it violates ANY of the shared constraints.
    # We build a compound filter as the logical-OR of the negated constraint expressions.
    invalid_filter = ~(
        F.expr(CONSTRAINTS["valid_customer_id"]) &
        F.expr(CONSTRAINTS["valid_email"]) &
        F.expr(CONSTRAINTS["valid_age"]) &
        F.expr(CONSTRAINTS["non_negative_spend"])
    )
    return dlt.read_stream("bronze_customers_raw").filter(invalid_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Silver Clean Table (declarative, with DLT constraints)
# MAGIC
# MAGIC This is the primary silver output. DLT enforces constraints and handles
# MAGIC incremental updates automatically.

# COMMAND ----------

@dlt.table(
    name="customers_clean",
    comment="Cleaned and deduplicated customer data — silver quality.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_customer_id", CONSTRAINTS["valid_customer_id"])
@dlt.expect_or_drop("valid_email",       CONSTRAINTS["valid_email"])
@dlt.expect_or_drop("valid_age",         CONSTRAINTS["valid_age"])
@dlt.expect_or_drop("non_negative_spend", CONSTRAINTS["non_negative_spend"])
def customers_clean():
    # Read from the upstream bronze live table
    raw = dlt.read_stream("bronze_customers_raw")

    # Apply transformations
    transformed = (
        raw
            .withColumn("first_name",      F.initcap(F.trim(F.col("first_name"))))
            .withColumn("last_name",       F.initcap(F.trim(F.col("last_name"))))
            .withColumn("email",           F.lower(F.trim(F.col("email"))))
            .withColumn("address",         F.trim(F.col("address")))
            .withColumn("city",            F.initcap(F.trim(F.col("city"))))
            .withColumn("state",           F.upper(F.trim(F.col("state"))))
            .withColumn("country",         F.upper(F.trim(F.col("country"))))
            .withColumn("customer_id",     F.col("customer_id").cast("integer"))
            .withColumn("age",             F.col("age").cast("integer"))
            .withColumn("total_purchases", F.col("total_purchases").cast("double"))
            .withColumn("signup_date",     F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
            .withColumn("_processed_timestamp", F.current_timestamp())
    )

    # Deduplicate: keep latest record per customer_id within each micro-batch
    dedup_window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_timestamp").desc())
    return (
        transformed
            .withColumn("_row_num", F.row_number().over(dedup_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Gold-ready aggregation view (optional, for reference)
# MAGIC
# MAGIC Example of a downstream live view derived from the silver clean table,
# MAGIC showing loyalty tier distribution — useful as a foundation for a gold layer.

# COMMAND ----------

@dlt.view(
    name="v_loyalty_tier_summary",
    comment="Customer count and average spend per loyalty tier.",
)
def v_loyalty_tier_summary():
    return (
        dlt.read("customers_clean")
            .groupBy("loyalty_tier")
            .agg(
                F.count("customer_id").alias("customer_count"),
                F.round(F.avg("total_purchases"), 2).alias("avg_total_purchases"),
                F.round(F.sum("total_purchases"), 2).alias("total_revenue"),
            )
            .orderBy("loyalty_tier")
    )
