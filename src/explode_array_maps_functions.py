import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating explode_and_map_function")\
    .master("local[8]")\
    .getOrCreate()
from pyspark.sql.functions import *
arrayData = [
        ('Stewart',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Edwin',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Divkar',['CSharp',''],{'hair':'red','eye':''}),
        ('Kishor',None,None),
        ('Chadan',['1','2'],{})]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()
# explode() on array column
from pyspark.sql.functions import explode
df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()
from pyspark.sql.functions import explode
df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()
# explode_outer() on array and map column
from pyspark.sql.functions import explode_outer
df.select(df.name,explode_outer(df.knownLanguages)).show()
df.select(df.name,explode_outer(df.properties)).show()

# posexplode() on array and map
from pyspark.sql.functions import posexplode
df.select(df.name,posexplode(df.knownLanguages)).show()
df.select(df.name,posexplode(df.properties)).show()
# posexplode_outer() on array and map
from pyspark.sql.functions import posexplode_outer
df.select("name",posexplode_outer("knownLanguages")).show()
df.select(df.name,posexplode_outer(df.properties)).show()