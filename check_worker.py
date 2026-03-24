"""Test PySpark with SPARK_LOCAL_IP forced to loopback."""
import os, sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
spark = (SparkSession.builder.master('local[1]')
    .config('spark.ui.enabled', 'false')
    .config('spark.sql.shuffle.partitions', '1')
    .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')

def test(label, fn):
    try:
        result = fn()
        print(f'PASS  {label}: {result}')
    except Exception as e:
        print(f'FAIL  {label}: {type(e).__name__}: {str(e)[:100]}')

test('range collect',    lambda: spark.range(2).collect())
test('createDF count',   lambda: spark.createDataFrame([(1,'a')], ['id','name']).count())
test('createDF collect', lambda: spark.createDataFrame([(1,'a')], ['id','name']).collect())

spark.stop()
print('Done')




