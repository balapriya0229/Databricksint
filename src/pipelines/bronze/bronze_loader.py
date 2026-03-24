"""
Bronze layer – ingest raw customer CSV files from a Unity Catalog Volume.

Uses Auto Loader (cloudFiles) within a Spark Declarative Pipeline to
incrementally read new files and land them as-is into a streaming table.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration – override via pipeline Spark conf if needed
# ---------------------------------------------------------------------------
VOLUME_PATH = "/Volumes/my_catalog/my_schema/customer_landing"
FILE_FORMAT = "csv"


# ---------------------------------------------------------------------------
# Bronze streaming table
# ---------------------------------------------------------------------------
@dp.table(
    name="bronze_customers",
    comment="Raw customer records ingested from the landing volume via Auto Loader.",
)
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def bronze_customers():
    """Incrementally ingest CSV files dropped into the customer landing volume."""
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821 – spark provided by SDP
        .option("cloudFiles.format", FILE_FORMAT)
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(VOLUME_PATH)
        .select("*", F.current_timestamp().alias("_ingested_at"))
    )
