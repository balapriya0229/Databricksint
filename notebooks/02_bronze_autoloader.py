# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: Bronze Layer — Ingest Customer Data with AutoLoader
# MAGIC
# MAGIC This notebook uses **Databricks AutoLoader** (`cloudFiles`) to incrementally ingest
# MAGIC customer CSV files from the landing zone into the **bronze** schema as a Delta table.
# MAGIC
# MAGIC AutoLoader automatically detects new files, infers schema, and handles exactly-once
# MAGIC ingestion using checkpoint tracking.
# MAGIC
# MAGIC **Pipeline:** Landing Zone (DBFS) → Bronze (Delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Paths and schema/table names
LANDING_ZONE       = "dbfs:/etl_demo/landing/customers/"
BRONZE_TABLE_PATH  = "dbfs:/etl_demo/delta/bronze/customers"
CHECKPOINT_PATH    = "dbfs:/etl_demo/checkpoints/bronze_customers"
BRONZE_CATALOG     = "etl_demo"
BRONZE_SCHEMA      = "bronze"
BRONZE_TABLE       = "customers_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog, Schema and ensure paths exist

# COMMAND ----------

# Create the Unity Catalog catalog and schema (if not already present)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {BRONZE_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoLoader Stream — Ingest CSV files into Bronze Delta Table
# MAGIC
# MAGIC Key AutoLoader options used:
# MAGIC - `cloudFiles.format` — source file format
# MAGIC - `cloudFiles.schemaLocation` — where AutoLoader persists inferred schema evolution
# MAGIC - `header` — first row is the header
# MAGIC - `inferSchema` — automatically detect column data types
# MAGIC - `cloudFiles.inferColumnTypes` — infer column types in AutoLoader mode

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = (
    spark.readStream
        .format("cloudFiles")                              # AutoLoader source format
        .option("cloudFiles.format", "csv")                # Source file format
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("inferSchema", "true")
        # Add metadata columns for audit/lineage
        .load(LANDING_ZONE)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Stream to Delta

# COMMAND ----------

bronze_query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH + "/data")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)           # Process all available files then stop
        .toTable(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")
)

bronze_query.awaitTermination()
print("Bronze ingestion complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Table

# COMMAND ----------

bronze_count = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}").count()
print(f"Total records in Bronze table: {bronze_count}")

# COMMAND ----------

display(
    spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}")
        .orderBy("customer_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Table Schema

# COMMAND ----------

spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}").printSchema()
