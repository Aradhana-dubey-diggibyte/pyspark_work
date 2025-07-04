import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating data_time")\
    .master("local[8]")\
    .getOrCreate()

from pyspark.sql.functions import *
#current_data
data = [("2025-07-01",), ("2025-12-31",), ("2024-02-29",)]
df = spark.createDataFrame(data, ["date"])
df = df.withColumn("date", to_date(df["date"]))
df.show()
df.withColumn("current_date", current_date()).show()
df.withColumn("current_timestamp",current_timestamp()).show()
df.withColumn("dateadd",date_add("date",5)).show()
df.withColumn("date_differennce",date_diff(current_date(),("date"))).show()
df.withColumn("years",year("date")).show()
df.withColumn("month",month("date")).show()
df.withColumn("Day",dayofmonth("date")).show()
df2 = spark.createDataFrame([("07/04/2025",)], ["date_str"])
df2.withColumn("converted_date", to_date("date_str", "MM/dd/yyyy")).show()
df.withColumn("formatted", date_format("date", "dd-MM-yyyy")).show()
