import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Creating transformation")\
    .master("local[8]")\
    .getOrCreate()

sc = spark.sparkContext
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data)
# map()
mapped_rdd = rdd.map(lambda x: x * 2)
print("Mapped RDD:", mapped_rdd.collect())
#filter()
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
print("Filtered RDD (even):", filtered_rdd.collect())
#flatMap()
rdd2 = sc.parallelize(["hey friend", "pyspark rdd"])
flat_mapped = rdd2.flatMap(lambda x: x.split(" "))
print("FlatMapped RDD:", flat_mapped.collect())
#distinct()
rdd3 = sc.parallelize([1, 2, 2, 3, 3, 3])
distinct_rdd = rdd3.distinct()
print("Distinct RDD:", distinct_rdd.collect())
#union()
rdd_a = sc.parallelize([1, 2])
rdd_b = sc.parallelize([3, 4])
union_rdd = rdd_a.union(rdd_b)
print("Union RDD:", union_rdd.collect())
#intersection()
rdd_x = sc.parallelize([1, 2, 3])
rdd_y = sc.parallelize([2, 3, 4])
intersection_rdd = rdd_x.intersection(rdd_y)
print("Intersection RDD:", intersection_rdd.collect())
#subtract()
subtract_rdd = rdd_x.subtract(rdd_y)
print("Subtract RDD:", subtract_rdd.collect())
#cartesian()
cartesian_rdd = rdd_a.cartesian(rdd_b)
print("Cartesian RDD:", cartesian_rdd.collect())
#groupBy()
grouped = rdd.groupBy(lambda x: x % 2).mapValues(list)
print("Grouped RDD (by even/odd):", grouped.collect())
#sortBy()
unsorted = sc.parallelize([5, 1, 3, 2])
sorted_rdd = unsorted.sortBy(lambda x: x)
print("Sorted RDD:", sorted_rdd.collect())
sc.stop()
