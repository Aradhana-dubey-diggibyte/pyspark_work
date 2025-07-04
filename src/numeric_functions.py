import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating numeric_function")\
    .master("local[8]")\
    .getOrCreate()
from pyspark.sql.functions import *
#data
data = [("Shayam", 88.6), ("Vishal", 76.3), ("Reshmika", -45.9)]
df = spark.createDataFrame(data, ["name", "score"])
df.show()
df.select(sum("score").alias("total_sum")).show()
df.select(avg("score").alias("Avg_score")).show()
df.withColumn("Rounded_score",round("score",0)).show()
df.withColumn("ABS_score",abs("score")).show()
df.select(max("score").alias("Max_score")).show()
df.select(min("score").alias("Min_score")).show()
