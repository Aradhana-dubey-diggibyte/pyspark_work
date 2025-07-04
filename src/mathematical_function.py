import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating mathematical_function")\
    .master("local[8]")\
    .getOrCreate()
from pyspark.sql.functions import *
data = [(4.5,), (-3.7,), (0.0,), (2.0,)]
df = spark.createDataFrame(data, ["value"])
df.show()
df.withColumn("abs_value", abs("value")).show()
df.withColumn("ceil_value", ceil("value")).show()
df.withColumn("floor_value", floor("value")).show()
df.withColumn("exp_value", exp("value")).show()
df.withColumn("log_value", log("value")).show()
df.withColumn("squared", pow("value", 2)).show()
df.withColumn("sqrt_value", sqrt("value")).show()
