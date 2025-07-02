import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import StorageLevel
spark = SparkSession.builder \
    .appName("Caching_persisting")\
    .master("local[*]")\
    .getOrCreate()

#Step 2: Load a sample DataFrame
data = [
    (1, "Rashik", "HR", 4000),
    (2, "vikas", "IT", 5000),
    (3, "Chanan", "IT", 5500),
    (4, "David", "HR", 4200),
    (5, "Edwin", "Finance", 6000)
]
columns = ["id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)

#Transformation (select high earners)
high_earners = df.filter(col("salary") > 4500)

#Cache the transformed DataFrame
high_earners.cache()

# Perform multiple actions (reuses cache)
print("First action (triggers cache):")
high_earners.show()

print("Second action (uses cache):")
high_earners.groupBy("department").count().show()

#Use persist with custom storage level (MEMORY_ONLY)
df.persist(StorageLevel.MEMORY_ONLY)

print("Another action on original df (uses persist):")
df.groupBy("department").avg("salary").show()

# Unpersist when no longer needed
df.unpersist()
high_earners.unpersist()
spark.stop()