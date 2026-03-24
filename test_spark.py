import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").config("spark.ui.enabled","false").getOrCreate()
df = spark.createDataFrame([(1,"hello")], ["id","name"])
print(df.collect())
spark.stop()
print("SUCCESS")
