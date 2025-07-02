import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Actions").master("local[*]").getOrCreate()
sc = spark.sparkContext
#data
rdd = sc.parallelize([10, 20, 30, 40, 50])

#collect() - returns all elements to driver (use only with small data)
print("collect():", rdd.collect())

#count() - total number of elements
print("count():", rdd.count())

# first() - first element
print("first():", rdd.first())

#take(n) - get first n elements
print("take(3):", rdd.take(3))

#reduce(func) - reduce all elements using function
from operator import add
sum_result = rdd.reduce(add)
print("reduce(add):", sum_result)

#takeSample(withReplacement, num)
sample = rdd.takeSample(False, 3)
print("takeSample():", sample)

# max() and min()
print("max():", rdd.max())
print("min():", rdd.min())

#sum()
print("sum():", rdd.sum())

#foreach(func) - apply a function (doesn't return result, just runs it)
print("foreach():")
rdd.foreach(lambda x: print(f"Value: {x}"))

rdd.saveAsTextFile("output_rdd_actions")

sc.stop()
