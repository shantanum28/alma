from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initializing a SparkSession
spark_session = SparkSession.builder \
    .appName("Divvy Trips Analysis") \
    .getOrCreate()

# Reading CSV data with inferred schema
df_inferred = spark_session.read.option("header", True).csv("Divvy_Trips_2015-Q1.csv")
print("DataFrame with inferred schema:")
df_inferred.printSchema()
print("Number of records:", df_inferred.count())

# Creating schema programmatically and reading CSV
schema_fields = [
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
]
programmatic_schema = StructType(schema_fields)
df_programmatic = spark_session.read.option("header", True).schema(programmatic_schema).csv("Divvy_Trips_2015-Q1.csv")
print("\nDataFrame with programmatically created schema:")
df_programmatic.printSchema()
print("Number of records:", df_programmatic.count())

# Attaching schema via DDL and reading CSV
ddl_schema_str = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"
df_ddl = spark_session.read.option("header", True).schema(ddl_schema_str).csv("Divvy_Trips_2015-Q1.csv")
print("\nDataFrame with schema attached via DDL:")
df_ddl.printSchema()
print("Number of records:", df_ddl.count())

# Selecting Gender and filtering
df_filtered = df_ddl.select("gender").filter((df_ddl["gender"] == "Female") | (df_ddl["gender"] == "Male"))
print("\nFiltered DataFrame:")
df_filtered.show(10)

# GroupingBy station to
df_grouped = df_ddl.groupBy("to_station_name").count()
print("\nGrouped DataFrame:")
df_grouped.show(10)

# Stopping SparkSession
spark_session.stop()