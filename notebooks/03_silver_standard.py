# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3: Silver Layer — Clean & Deduplicate (Standard PySpark Approach)
# MAGIC
# MAGIC This notebook reads from the **bronze** table, applies data quality transformations,
# MAGIC deduplicates records, and writes the clean data into the **silver** schema using
# MAGIC standard PySpark batch operations (no DLT).
# MAGIC
# MAGIC **Pipeline:** Bronze → (clean + dedup) → Silver
# MAGIC
# MAGIC **Transformations applied:**
# MAGIC - Drop rows with null `customer_id` or `email`
# MAGIC - Standardise text columns (trim whitespace, proper case for names)
# MAGIC - Normalise `email` to lowercase
# MAGIC - Cast and validate numeric columns (`age`, `total_purchases`)
# MAGIC - Deduplicate by `customer_id`, keeping the most-recently ingested record
# MAGIC - Add `_processed_timestamp` audit column

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

BRONZE_CATALOG  = "etl_demo"
BRONZE_SCHEMA   = "bronze"
BRONZE_TABLE    = "customers_raw"

SILVER_CATALOG  = "etl_demo"
SILVER_SCHEMA   = "silver"
SILVER_TABLE    = "customers_clean"

SILVER_TABLE_PATH = "dbfs:/etl_demo/delta/silver/customers_clean"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {SILVER_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_df = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")
print(f"Records read from Bronze: {bronze_df.count()}")
display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Drop rows missing key fields

# COMMAND ----------

cleaned_df = bronze_df.filter(
    F.col("customer_id").isNotNull() &
    F.col("email").isNotNull() &
    F.col("first_name").isNotNull() &
    F.col("last_name").isNotNull()
)
print(f"Records after null-key filter: {cleaned_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Standardise and cast columns

# COMMAND ----------

cleaned_df = (
    cleaned_df
        # Trim whitespace and apply proper-case to name fields
        .withColumn("first_name",      F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",       F.initcap(F.trim(F.col("last_name"))))
        # Normalise email to lowercase and trim
        .withColumn("email",           F.lower(F.trim(F.col("email"))))
        # Trim address fields
        .withColumn("address",         F.trim(F.col("address")))
        .withColumn("city",            F.initcap(F.trim(F.col("city"))))
        .withColumn("state",           F.upper(F.trim(F.col("state"))))
        .withColumn("country",         F.upper(F.trim(F.col("country"))))
        # Cast numeric columns
        .withColumn("customer_id",     F.col("customer_id").cast("integer"))
        .withColumn("age",             F.col("age").cast("integer"))
        .withColumn("total_purchases", F.col("total_purchases").cast("double"))
        # Cast signup_date to date
        .withColumn("signup_date",     F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
        # Add processing timestamp
        .withColumn("_processed_timestamp", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Remove invalid rows (age or total_purchases out of range)

# COMMAND ----------

cleaned_df = cleaned_df.filter(
    (F.col("age") > 0) & (F.col("age") < 120) &
    (F.col("total_purchases") >= 0)
)
print(f"Records after range validation: {cleaned_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Deduplicate by `customer_id`
# MAGIC
# MAGIC Keep the record with the latest `_ingest_timestamp` for each `customer_id`.

# COMMAND ----------

dedup_window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_timestamp").desc())

silver_df = (
    cleaned_df
        .withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
)

print(f"Records after deduplication: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Write to Silver Delta Table

# COMMAND ----------

(
    silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}")
)
print(f"Silver table written: {SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Table

# COMMAND ----------

silver_count = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}").count()
print(f"Total records in Silver table: {silver_count}")

display(
    spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}")
        .orderBy("customer_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver vs Bronze record count comparison

# COMMAND ----------

print(f"Bronze record count : {bronze_df.count()}")
print(f"Silver record count : {silver_count}")
print(f"Duplicates removed  : {bronze_df.count() - silver_count}")
