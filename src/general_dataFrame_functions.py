import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("general_dataframe_function")\
    .master("local[*]")\
    .getOrCreate()
df = spark.read.csv("Employee-Q1.csv",multiLine=True,header=True, inferSchema=True)
df.show()

print("\n collect():")
rows = df.collect()
for row in rows:
    print(row)
print("\n printSchema():")
df.printSchema()
print("\n Filter(salary>3500):")
df.filter(df.salary>3500)
df.show()
#count
print("Total rows:", df.count())
#sort
df.sort(df.salary.desc()).show()
#like
df.filter(df.name.like("A%")).show()
#describe
df.describe().show()
#columns
print(df.columns)
#take
print(df.take(2))
spark.stop()