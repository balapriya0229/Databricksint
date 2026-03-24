"""
Unit tests for customer_pipeline.transformations.

Run with:  pytest customer_pipeline/tests/test_transformations.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from customer_pipeline.transformations import (
    add_full_name,
    deduplicate_customers,
    drop_rows_missing_required_fields,
    normalise_email,
    trim_string_columns,
    apply_silver_transformations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("customer_pipeline_tests")
        .getOrCreate()
    )


CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)


@pytest.fixture()
def sample_df(spark):
    """Return a small DataFrame with intentional quality issues."""
    from datetime import datetime

    rows = [
        ("C001", "  Alice@Example.COM ", " Alice ", "Smith", datetime(2025, 1, 1)),
        ("C002", "bob@test.com", "Bob", "Jones", datetime(2025, 2, 1)),
        ("C001", "alice_new@example.com", "Alice", "Smith", datetime(2025, 3, 1)),
        ("C003", None, "Charlie", "Brown", datetime(2025, 1, 1)),
        (None, "nobody@test.com", "No", "Id", datetime(2025, 1, 1)),
        ("C004", "dave@test.com", "", "Lee", datetime(2025, 1, 1)),
    ]
    return spark.createDataFrame(rows, schema=CUSTOMER_SCHEMA)


# ---------------------------------------------------------------------------
# Tests – trim_string_columns
# ---------------------------------------------------------------------------
class TestTrimStringColumns:
    def test_removes_leading_trailing_spaces(self, spark):
        df = spark.createDataFrame([("  hi  ", " there ")], ["a", "b"])
        result = trim_string_columns(df).collect()
        assert result[0]["a"] == "hi"
        assert result[0]["b"] == "there"


# ---------------------------------------------------------------------------
# Tests – normalise_email
# ---------------------------------------------------------------------------
class TestNormaliseEmail:
    def test_lowercases_and_trims(self, spark):
        df = spark.createDataFrame(
            [("  Alice@Example.COM  ",)], ["email"]
        )
        result = normalise_email(df).collect()
        assert result[0]["email"] == "alice@example.com"


# ---------------------------------------------------------------------------
# Tests – drop_rows_missing_required_fields
# ---------------------------------------------------------------------------
class TestDropRowsMissingRequiredFields:
    def test_drops_null_customer_id(self, sample_df):
        result = drop_rows_missing_required_fields(sample_df)
        ids = {row["customer_id"] for row in result.collect()}
        assert None not in ids

    def test_drops_empty_string_first_name(self, sample_df):
        result = drop_rows_missing_required_fields(sample_df)
        names = {row["first_name"] for row in result.collect()}
        assert "" not in names

    def test_drops_null_email(self, sample_df):
        result = drop_rows_missing_required_fields(sample_df)
        emails = [row["email"] for row in result.collect()]
        assert None not in emails


# ---------------------------------------------------------------------------
# Tests – deduplicate_customers
# ---------------------------------------------------------------------------
class TestDeduplicateCustomers:
    def test_keeps_latest_per_customer(self, sample_df):
        deduped = deduplicate_customers(sample_df)
        c001_rows = deduped.filter(F.col("customer_id") == "C001").collect()
        assert len(c001_rows) == 1
        assert c001_rows[0]["email"] == "alice_new@example.com"


# ---------------------------------------------------------------------------
# Tests – add_full_name
# ---------------------------------------------------------------------------
class TestAddFullName:
    def test_concatenates_names(self, spark):
        df = spark.createDataFrame(
            [("Alice", "Smith")], ["first_name", "last_name"]
        )
        result = add_full_name(df).collect()
        assert result[0]["full_name"] == "Alice Smith"


# ---------------------------------------------------------------------------
# Tests – full pipeline
# ---------------------------------------------------------------------------
class TestApplySilverTransformations:
    def test_end_to_end(self, sample_df):
        result = apply_silver_transformations(sample_df)
        rows = result.collect()

        # C003 dropped (null email), None-id dropped, C004 dropped (empty first_name)
        customer_ids = {r["customer_id"] for r in rows}
        assert customer_ids == {"C001", "C002"}

        # Dedup: C001 keeps latest
        c001 = [r for r in rows if r["customer_id"] == "C001"][0]
        assert c001["email"] == "alice_new@example.com"
        assert c001["full_name"] == "Alice Smith"

    def test_no_extra_columns(self, sample_df):
        result = apply_silver_transformations(sample_df)
        assert "_row_num" not in result.columns
