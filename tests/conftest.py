"""Shared fixtures for local PySpark tests."""

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure src/ is on the path even when tests/ has an __init__.py (package-mode
# import) which can bypass the pythonpath setting in pyproject.toml.
_src = str(Path(__file__).resolve().parents[1] / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

# Force Spark to bind all sockets to loopback so Python workers always connect
# via 127.0.0.1 regardless of the machine's DNS hostname resolution.
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Pin PySpark worker processes to the same interpreter as the driver so they
# find the venv packages regardless of how pytest is invoked (CLI, VS Code, CI).
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# pytest-cov sets COV_CORE_* env vars so it can measure coverage in
# subprocesses via a .pth sitecustomize hook.  PySpark worker processes
# inherit those vars and the hook crashes them (EOFException / worker exited).
# Delete the keys here — before the JVM starts — so workers never see them.
# Coverage in the driver process is already running and is unaffected.
for _key in [k for k in list(os.environ) if k.startswith("COV_CORE")]:
    del os.environ[_key]


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> SparkSession:
    """Create a local Spark session for deterministic unit tests."""
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("databricksint-local-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    request.addfinalizer(spark_session.stop)
    return spark_session
