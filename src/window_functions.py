import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating window_function")\
    .master("local[8]")\
    .getOrCreate()
simpleData = (("Aradhana", "Sales", 3000), \
              ("Mamath", "Sales", 4600), \
              ("Reshmika", "Sales", 4100), \
              ("Marry", "Finance", 3000), \
              ("Edwin", "Sales", 3000), \
              ("Sandeep", "Finance", 3300), \
              ("Divkar", "Finance", 3900), \
              ("Shreya", "Marketing", 3000), \
              ("Kumar", "Marketing", 2000), \
              ("kishore", "Sales", 4100) \
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)
# row_number()
#we need to import window and row number
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)
# rank()
from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()
# dense_rank()
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()
# lag()
from pyspark.sql.functions import lag
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

# lead()
from pyspark.sql.functions import lead
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()