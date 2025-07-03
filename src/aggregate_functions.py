import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Creating aggregate_function")\
    .master("local[8]")\
    .getOrCreate()
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)
#approx_count_didtinct
print("approx_count_distinct: " + \
      str(df.select(approx_count_distinct("salary")).collect()[0][0]))
#avg
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
# collect_list
df.select(collect_list("department")).show(truncate=False)
# collect_set
df.select(collect_set("salary")).show(truncate=False)
#countDistinct
df2= df.select(count_distinct("department", "salary"))
df2.show()
print("Distinct of two columns: "+str(df2.collect()[0][0]))
# first
df.select(first("salary")).show(truncate=False)
# last
df.select(last("salary")).show(truncate=False)
df.select(max("salary")).show(truncate=False)
df.select(min("salary")).show(truncate=False)
df.select(mean("salary")).show(truncate=False)
df.select(sum("salary")).show(truncate=False)
