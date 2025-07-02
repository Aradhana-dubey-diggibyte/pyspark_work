import os
os.environ['PYSPARK_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\AradhanaDubey\AppData\Local\Programs\Python\Python311\python.exe'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("pysaprk_sql")\
    .master("local[8]")\
    .getOrCreate()

#Start Spark session
spark = SparkSession.builder.appName("HRAnalytics").getOrCreate()

#Sample employee data
data = [
    (1, "Ankit", "HR", 5000),
    (2, "Anurag", "IT", 4500),
    (3, "Chadan", "HR", None),       # Missing salary
    (4, "David", "Finance", 6000),
    (5, "Edwin", "IT", 4000),
    (6, "Divkar", "Finance", None)     # Missing salary
]
columns = ["id", "name", "department", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

#Local Temp View - Clean data (remove null salaries)
clean_df = df.filter(col("salary").isNotNull())
clean_df.createOrReplaceTempView("clean_employees")

#Run SQL query on local temp view
print("Filtered IT Department Employees (Local Temp View):")
spark.sql("""
    SELECT * FROM clean_employees
    WHERE department = 'IT'
""").show()

# Create department summary using SQL
summary_df = spark.sql("""
    SELECT department, COUNT(*) AS num_employees, AVG(salary) AS avg_salary
    FROM clean_employees
    GROUP BY department
""")

# Register summary as Global Temp View
summary_df.createGlobalTempView("department_salary_summary")

#Access Global Temp View (works across sessions)
print("Department-wise Salary Summary (Global Temp View):")
spark.sql("SELECT * FROM global_temp.department_salary_summary").show()

spark.stop()
