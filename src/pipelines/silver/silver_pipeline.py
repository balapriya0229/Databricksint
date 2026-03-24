"""
Silver layer – clean, deduplicate, and enrich customer data.

Reads from the bronze streaming table and applies pure transformation
functions defined in ``transformations.py``.  Data-quality expectations
enforce row-level contracts; failing rows are dropped.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from pipelines.silver.transformations import apply_silver_transformations


# ---------------------------------------------------------------------------
# Silver materialized view
# ---------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_customers",
    comment="Cleaned and deduplicated customer records.",
)
@dp.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%.%'")
@dp.expect_or_drop("valid_name", "full_name IS NOT NULL AND full_name != ''")
def silver_customers():
    """Apply the full silver transformation chain to bronze data."""
    bronze_df = spark.read.table("bronze_customers")  # noqa: F821
    return apply_silver_transformations(bronze_df)
