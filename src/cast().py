import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating cast_function")\
    .master("local[8]")\
    .getOrCreate()
from pyspark.sql.functions import *
simpleData = [("James",34,"2006-01-01","true","M",3000.60),
    ("Michael",33,"1980-01-10","true","F",3300.80),
    ("Robert",37,"06-01-1992","false","M",5000.50)
  ]

columns = ["firstname","age","jobStartDate","isGraduated","gender","salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
df2 = df.withColumn("age",col("age").cast(StringType())) \
    .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
df2.printSchema()
df3 = df2.selectExpr("cast(age as int) age",
    "cast(isGraduated as string) isGraduated",
    "cast(jobStartDate as string) jobStartDate")
df3.printSchema()

from pyspark.sql.functions import to_date, when, col

df4 = df.withColumn("jobStartDate",
        when(col("jobStartDate").rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}$"),
             to_date(col("jobStartDate"), "dd-MM-yyyy"))
        .otherwise(to_date(col("jobStartDate"), "yyyy-MM-dd"))
    )
