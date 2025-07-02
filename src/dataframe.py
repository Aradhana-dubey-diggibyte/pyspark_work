import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

#creating a dataframe using createdataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import upper
from pyspark.sql.functions import when
spark = SparkSession.builder \
    .appName("Createing dataframe")\
    .master("local[*]")\
    .getOrCreate()
data = [
    (1, "Aradhana", "DB001","IN", 10000,22),
    (2,"Divar", "DBO02", "SA",12000,22),
    (3,"Edwin","DB003", "IN", 10000,21 ),
    (4,"Sharya","DB004","USA",10000,22)

]
columns = ["EmployeeID", "EmployeeName", "Department", "Country", "Salary", "Age"]
df = spark.createDataFrame(data, columns)
df.show()
df.printSchema()
# selecting the specific columns
df.select("EmployeeName","Salary").show()
#Using filter() and where ()
df.filter(df.Salary > 10000).show()
df.where(df.Country =="IN").show
#.withColumn()
df.withColumn("Salary_Inc",col("Salary") + 5000).show()
df.withColumn("Bonus", col("Salary") * 0.10).show()
df.withColumn("Uppercaseempname", upper(col("EmployeeName"))).show()
df.withColumn("AgeGroup" , when(col("Age") < 19, "Young")).show
#droping a column
df.drop("Age").show()
#alias() to rename
df.select(col("Salary").alias("Income")).show()
