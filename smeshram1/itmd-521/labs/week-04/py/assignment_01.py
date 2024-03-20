from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, count

spark = SparkSession.builder \
    .appName("Assignment 01 - Python") \
    .getOrCreate()

#Read csv file into a Spark Data frame
inferred_schema_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/Divvy_Trips_2015-Q1.csv")

# Print the schema inferred
print("Schema inferred:")
inferred_schema_df.printSchema()

# Display number of records
print("Number of records:", inferred_schema_df.count())


# define schema programmatically using StructFields
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

#read csv file using programmatically defined schema into a spark data frame
programmatic_schema_df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv("data/Divvy_Trips_2015-Q1.csv")

# Print the programmatically defined schema
print("Programmatically defined schema:")
programmatic_schema_df.printSchema()

# Display number of records
print("Number of records:", programmatic_schema_df.count())


# Attach schema via DDL and read the CSV file
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, " \
             "from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, " \
             "usertype STRING, gender STRING, birthyear INT"
ddl_schema_df = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .schema(ddl_schema) \
        .csv("data/Divvy_Trips_2015-Q1.csv")

# Print the schema via DDL
print("Schema attached via DDL:")
ddl_schema_df.printSchema()

# Display number of records
print("Number of records:", ddl_schema_df.count())

 # Select Gender based on the last name first letter
df_with_gender = programmatic_schema_df.withColumn("Gender", when(col("to_station_name").rlike("[A-K]"), "Female").otherwise("Male"))

# GroupBy the field "to_station_name"
df_grouped = df_with_gender.groupBy("to_station_name").agg(count("to_station_name").alias("Number_of_Trips"))

# Show 10 records of the resulting DataFrame
print("Grouped by to_station_name:")
df_grouped.show(10)

# Stop SparkSession
spark.stop()