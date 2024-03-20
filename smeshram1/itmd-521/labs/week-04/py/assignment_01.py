from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initializing SparkSession
spark = SparkSession.builder \
    .appName("Unique Divvy Trips Analysis") \
    .getOrCreate()

# Reading CSV data with inferred schema
df_inferred_schema = spark.read.option("header", True).csv("Divvy_Trips_2015-Q1.csv")
print("DataFrame with inferred schema:")
df_inferred_schema.printSchema()
print("Number of records read:", df_inferred_schema.count())

# Creating schema programmatically and reading CSV
programmatic_schema = StructType([
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
    StructField("birthyear", StringType(), True)
])
df_programmatic_schema = spark.read.option("header", True).schema(programmatic_schema).csv("Divvy_Trips_2015-Q1.csv")
print("\nDataFrame with programmatically created schema:")
df_programmatic_schema.printSchema()
print("Number of records processed:", df_programmatic_schema.count())

# Attaching schema via DDL and reading CSV
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"
df_ddl_schema = spark.read.option("header", True).schema(ddl_schema).csv("Divvy_Trips_2015-Q1.csv")
print("\nDataFrame with schema attached via DDL:")
df_ddl_schema.printSchema()
print("Number of records processed:", df_ddl_schema.count())

# Selecting Gender and filtering
df_filtered = df_ddl_schema.select("gender").filter((df_ddl_schema["gender"] == "Female") | (df_ddl_schema["gender"] == "Male"))
print("\nFiltered DataFrame:")
df_filtered.show(10)

# GroupingBy station
df_grouped = df_ddl_schema.groupBy("to_station_name").count()
print("\nGrouped DataFrame:")
df_grouped.show(10)

# Stopping SparkSession
spark.stop()