"""Unit tests for pipelines.silver.transformations."""

import pytest
from pyspark.sql import functions as F
from pipelines.silver.transformations import (
    add_full_name,
    apply_silver_transformations,
    deduplicate_customers,
    drop_rows_missing_required_fields,
    normalise_email,
    trim_string_columns,
)


@pytest.fixture()
def sample_df(spark):
    """Return a small DataFrame with intentional quality issues."""
    return spark.sql("""
        SELECT * FROM VALUES
            ('C001', '  Alice@Example.COM ', ' Alice ', 'Smith', TIMESTAMP '2025-01-01 00:00:00'),
            ('C002', 'bob@test.com',         'Bob',     'Jones', TIMESTAMP '2025-02-01 00:00:00'),
            ('C001', 'alice_new@example.com','Alice',   'Smith', TIMESTAMP '2025-03-01 00:00:00'),
            ('C003', NULL,                   'Charlie', 'Brown', TIMESTAMP '2025-01-01 00:00:00'),
            (NULL,   'nobody@test.com',      'No',      'Id',    TIMESTAMP '2025-01-01 00:00:00'),
            ('C004', 'dave@test.com',        '',        'Lee',   TIMESTAMP '2025-01-01 00:00:00')
        AS t(customer_id, email, first_name, last_name, updated_at)
    """)


class TestTrimStringColumns:
    def test_removes_leading_trailing_spaces(self, spark):
        df = spark.sql("SELECT * FROM VALUES ('  hi  ', ' there ') AS t(a, b)")
        result = trim_string_columns(df).collect()
        assert result[0]["a"] == "hi"
        assert result[0]["b"] == "there"


class TestNormaliseEmail:
    def test_lowercases_and_trims(self, spark):
        df = spark.sql("SELECT * FROM VALUES ('  Alice@Example.COM  ') AS t(email)")
        result = normalise_email(df).collect()
        assert result[0]["email"] == "alice@example.com"


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


class TestDeduplicateCustomers:
    def test_keeps_latest_per_customer(self, sample_df):
        deduped = deduplicate_customers(sample_df)
        c001_rows = deduped.filter(F.col("customer_id") == "C001").collect()
        assert len(c001_rows) == 1
        assert c001_rows[0]["email"] == "alice_new@example.com"


class TestAddFullName:
    def test_concatenates_names(self, spark):
        df = spark.sql("SELECT * FROM VALUES ('Alice', 'Smith') AS t(first_name, last_name)")
        result = add_full_name(df).collect()
        assert result[0]["full_name"] == "Alice Smith"


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
