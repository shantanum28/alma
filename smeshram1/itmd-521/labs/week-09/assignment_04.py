from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("assignment_04") \
    .getOrCreate()

# Part I
# Read the employees table into a DataFrame
employees_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "employees") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .load()

# Display the count of the number of records in the DF
print("Number of records in employees table:", employees_df.count())

# Display the schema of the Employees Table from the DF
employees_df.printSchema()

# Create a DataFrame of the top 10,000 employee salaries (sort DESC) from the salaries table
salaries_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "(SELECT * FROM salaries ORDER BY salary DESC LIMIT 10000) as temp") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .load()

# Write the DataFrame back to database to a new table called: aces
salaries_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "aces") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .mode("errorifexists") \
    .save()

# Write the DataFrame out to the local system as a CSV and save it to local system using snappy compression
salaries_df.write.format("csv") \
    .option("compression", "snappy") \
    .mode("errorifexists") \
    .save("csv")

# Part II
# Create a JDBC read without using the dbtables option, instead use a query directly
titles_df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .load()

# Add a temp column stating if the senior engineer employee is current or left
titles_df = titles_df.withColumn("status", when(col("to_date") == "9999-01-01", "current").otherwise("left"))

# Count how many senior engineers have left and how many are current
count_df = titles_df.groupBy("status").count()
count_df.show()

# Create a PySpark SQL table of just the Senior Engineers information that have left the company
titles_df.createOrReplaceTempView("left_tempview")

# Create a PySpark SQL tempView of just the Senior Engineers information that have left the company
left_engineers_df = spark.sql("SELECT * FROM left_tempview WHERE status = 'left'")

# Write each of the three prior options back to the database using the DataFrame Writer (save) function
titles_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "left_table") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .mode("errorifexists") \
    .save()

left_engineers_df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "left_df") \
    .option("user", "worker") \
    .option("password", "cluster") \
    .mode("errorifexists") \
    .save()

# Close SparkSession
spark.stop()
