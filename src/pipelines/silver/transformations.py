"""
Customer data transformations.

Pure PySpark functions with no pipeline dependencies.
Each function accepts a DataFrame and returns a DataFrame,
making them fully unit-testable in isolation.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS: list[str] = [
    "customer_id",
    "email",
    "first_name",
    "last_name",
]

DEDUP_KEY: str = "customer_id"
DEDUP_ORDER_COL: str = "updated_at"


# ---------------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------------
def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def normalise_email(df: DataFrame) -> DataFrame:
    """Lower-case the *email* column and remove surrounding whitespace."""
    return df.withColumn("email", F.lower(F.trim(F.col("email"))))


def drop_rows_missing_required_fields(df: DataFrame) -> DataFrame:
    """Drop rows where any REQUIRED_COLUMNS value is null or empty string."""
    condition = F.lit(True)
    for col_name in REQUIRED_COLUMNS:
        condition = condition & (
            F.col(col_name).isNotNull() & (F.col(col_name) != "")
        )
    return df.filter(condition)


# ---------------------------------------------------------------------------
# De-duplication
# ---------------------------------------------------------------------------
def deduplicate_customers(
    df: DataFrame,
    key: str = DEDUP_KEY,
    order_col: str = DEDUP_ORDER_COL,
) -> DataFrame:
    """Keep only the most-recent row per *key*, ordered by *order_col* desc."""
    window = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Derived columns
# ---------------------------------------------------------------------------
def add_full_name(df: DataFrame) -> DataFrame:
    """Create a *full_name* column from first_name and last_name."""
    return df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def apply_silver_transformations(df: DataFrame) -> DataFrame:
    """
    Apply the full bronze-to-silver transformation chain.

    Order matters:
      1. Trim strings
      2. Normalise email
      3. Drop invalid rows
      4. De-duplicate
      5. Add derived columns
    """
    return (
        df.transform(trim_string_columns)
        .transform(normalise_email)
        .transform(drop_rows_missing_required_fields)
        .transform(deduplicate_customers)
        .transform(add_full_name)
    )
