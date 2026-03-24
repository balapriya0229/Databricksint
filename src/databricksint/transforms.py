"""Example transformation functions for local PySpark unit tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_names(df: DataFrame) -> DataFrame:
    """Trim and uppercase `name`, preserving `id` and `country` fields."""
    return df.select(
        "id",
        F.upper(F.trim(F.col("name"))).alias("name"),
        "country",
    )


def orders_by_country(df: DataFrame) -> DataFrame:
    """Aggregate row counts by country and return as `country`, `order_count`."""
    return df.groupBy("country").agg(F.count(F.lit(1)).alias("order_count"))
