import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Creating string_function")\
    .master("local[8]")\
    .getOrCreate()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Sample Data
data = [("   Aradhana Dubey   ", "123-456-7890", "Aradhana123@gmail.com")]
df = spark.createDataFrame(data, ["name", "phone", "email"])

# Function 1: trim()
df.select(trim("name").alias("trimmed")).show(truncate=False)

# Function 2: ltrim()
df.select(ltrim("name").alias("left_trimmed")).show(truncate=False)

# Function 3: rtrim()
df.select(rtrim("name").alias("right_trimmed")).show(truncate=False)

# Function 4: upper()
df.select(upper("name").alias("upper_case")).show(truncate=False)

# Function 5: lower()
df.select(lower("name").alias("lower_case")).show(truncate=False)

# Function 6: initcap()
df.select(initcap("name").alias("title_case")).show(truncate=False)

# Function 7: substring()
df.select(substring("phone", 1, 3).alias("first_3_digits")).show(truncate=False)

# Function 8: substring_index()
df.select(substring_index("email", "@", -1).alias("email_domain")).show(truncate=False)

# Function 9: split()
df.select(split("phone", "-").alias("split_phone")).show(truncate=False)

# Function 10: repeat()
df.select(repeat("name", 2).alias("repeated_name")).show(truncate=False)

# Function 11: lpad()
df.select(lpad("phone", 15, "0").alias("left_padded")).show(truncate=False)

# Function 12: rpad()
df.select(rpad("phone", 15, "#").alias("right_padded")).show(truncate=False)

# Function 13: regex_replace()
df.select(regex_replace("email", "@.*", "@***").alias("masked_email")).show(truncate=False)

# Function 14: regex_extract()
df.select(regex_extract("email", r'(\w+)', 0).alias("username")).show(truncate=False)

# Function 15: length()
df.select(length("email").alias("email_length")).show(truncate=False)

# Function 16: instr()
df.select(instr("email", "@").alias("position_of_at")).show(truncate=False)



