from chispa import assert_df_equality

from databricksint.transforms import normalize_names, orders_by_country


def test_normalize_names(spark):
    source = spark.createDataFrame(
        [
            (1, "  alice  ", "US"),
            (2, "bob", "IN"),
        ],
        ["id", "name", "country"],
    )

    actual = normalize_names(source)

    expected = spark.createDataFrame(
        [
            (1, "ALICE", "US"),
            (2, "BOB", "IN"),
        ],
        ["id", "name", "country"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_column_order=False)


def test_orders_by_country(spark):
    source = spark.createDataFrame(
        [
            ("US",),
            ("US",),
            ("IN",),
        ],
        ["country"],
    )

    actual = orders_by_country(source)

    expected = spark.createDataFrame(
        [
            ("US", 2),
            ("IN", 1),
        ],
        ["country", "order_count"],
    )

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_column_order=False)
