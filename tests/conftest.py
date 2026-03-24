"""Shared fixtures for local PySpark tests."""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> SparkSession:
    """Create a local Spark session for deterministic unit tests."""
    spark_session = (
        SparkSession.builder.master("local[2]")
        .appName("databricksint-local-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    request.addfinalizer(spark_session.stop)
    return spark_session
