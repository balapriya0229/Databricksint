"""Run a minimal Spark job and redirect worker stderr to a file."""
import os, sys, tempfile

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Write worker errors to a temp file so we can read them
worker_log = r'd:\test\int\Databricksint\worker_stderr.log'
os.environ['PYSPARK_WORKER_FAULTHANDLER_TIMEOUT'] = '10'

from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .master('local[1]')
    .appName('debug')
    .config('spark.ui.enabled', 'false')
    .config('spark.sql.shuffle.partitions', '1')
    .config('spark.python.worker.reuse', 'false')
    .getOrCreate()
)

# Redirect Python worker stderr via log4j config
spark.sparkContext.setLogLevel('ERROR')

try:
    df = spark.createDataFrame([(1, 'alice', 'US')], ['id', 'name', 'country'])
    rows = df.collect()
    print('COLLECTED:', rows)
except Exception as e:
    print('EXCEPTION:', type(e).__name__, str(e)[:300])
finally:
    spark.stop()
    print('DONE')

