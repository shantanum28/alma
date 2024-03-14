from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import when, lit, current_date
from datetime import date

# Create a SparkSession
spark = SparkSession.builder \
    .appName("assignment_04") \
    .getOrCreate()

# Read the employees table into a DataFrame
employees_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "employees") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .load()

# Display the count of the number of records in the DataFrame
print("Number of records in employees DataFrame:", employees_df.count())

# Display the schema of the Employees Table from the DataFrame
employees_df.printSchema()

# Create a DataFrame of the top 10,000 employee salaries (sort DESC) from the salaries table
top_salaries_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "salaries") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .load() \
    .orderBy("salary", ascending=False) \
    .limit(10000)

# Write the DataFrame back to the database to a new table called "aces"
top_salaries_df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "aces") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .mode("overwrite") \
    .save()

# Write the DataFrame out to the local system as a CSV and save it to the local system using snappy compression
current_dir = os.getcwd()
top_salaries_df.write \
    .option("compression", "snappy") \
    .csv(f"{current_dir}/top_salaries.csv.snappy")

# Create a JDBC read without using the dbtables option, instead use a query directly to select from the titles table where employees title equals Senior Engineer
senior_engineers_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("query", "SELECT * FROM titles WHERE title = 'Senior Engineer'") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .load()

# Create a PySpark query to add to the prior step's DataFrame a temp column stating if the senior engineer employee is still with the company (to_date = 9999-01-01) and has left (to_date value will be less than today's date)
today = date.today().strftime("%Y-%m-%d")

senior_engineers_df = senior_engineers_df.withColumn(
    "status",
    when(senior_engineers_df.to_date == "9999-01-01", lit("current"))
    .otherwise(
        when(senior_engineers_df.to_date < lit(today), lit("left"))
        .otherwise(lit("unknown"))
    ),
)

# Issue a count of how many senior engineers have left and how many are current
left_count = senior_engineers_df.filter(senior_engineers_df.status == "left").count()
current_count = senior_engineers_df.filter(senior_engineers_df.status == "current").count()
print(f"Number of Senior Engineers who have left: {left_count}")
print(f"Number of current Senior Engineers: {current_count}")

# Create a PySpark SQL table of just the Senior Engineers information that have left the company
senior_engineers_df.filter(senior_engineers_df.status == "left").createOrReplaceTempView("left_table")

# Create a PySpark SQL tempView of just the Senior Engineers information that have left the company
senior_engineers_df.filter(senior_engineers_df.status == "left").createOrReplaceTempView("left_tempview")

# Create a PySpark DataFrame of just the Senior Engineers information that have left the company
left_df = senior_engineers_df.filter(senior_engineers_df.status == "left")

# Write each of the three prior options back to the database using the DataFrame Writer (save) function creating a table named: left-table left_table, left-tempview left_tempview, and left-df left_df respectively
left_df.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost/employees") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "left_df") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .mode("overwrite") \
    .save()

spark.sql("CREATE TABLE left_table AS SELECT * FROM left_table")
spark.sql("CREATE TEMPORARY VIEW left_tempview AS SELECT * FROM left_tempview")

# Repeat the previous command that writes the DataFrame to the database, but set the mode type to errorifexists -- take a screenshot of the error message generated and place it here
try:
    left_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost/employees") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "left_df") \
        .option("user", "myuser") \
        .option("password", "mypassword") \
        .mode("errorifexists") \
        .save()
except Exception as e:
    print("Error:", e)