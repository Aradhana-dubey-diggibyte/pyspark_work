import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, upper, when
spark = SparkSession.builder.appName("withcolumn()function")\
    .master("local[*]")\
    .getOrCreate()
data = [
    (1, "Aradhana", 5000),
    (2, "Sandeep", 4000),
    (3, "Vishal", 2500)
]
columns = ["id", "name", "salary"]
df = spark.createDataFrame(data, columns)
print("Original DataFrame:")
df.show()
#Add a new column (constant value)
df1 = df.withColumn("country", lit("India"))
# Modify 'name' column to uppercase
df2 = df1.withColumn("name", upper(col("name")))
#Add conditional column using when()
df3 = df2.withColumn("status", when(col("salary") > 4000, "High").otherwise("Low"))
#Cast salary to string (type conversion)
df4 = df3.withColumn("salary_str", col("salary").cast("string"))
print("Modified DataFrame:")
df4.show()
spark.stop()
