from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark_session = SparkSession.builder \
    .appName("Divvy Trips Analysis") \
    .getOrCreate()

# Define the schema
custom_schema = StructType([
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

# Read CSV with inferred schema
df_inferred_schema = spark_session.read.csv("Divvy_Trips_2015-Q1.csv", header=True, inferSchema=True)
print("Inferred Schema:")
df_inferred_schema.printSchema()
print("Number of records:", df_inferred_schema.count())

# Read CSV with programmatically defined schema
df_programmatic_schema = spark_session.read.csv("Divvy_Trips_2015-Q1.csv", header=True, schema=custom_schema)
print("\nProgrammatic Schema:")
df_programmatic_schema.printSchema()
print("Number of records:", df_programmatic_schema.count())

# Read CSV with schema via DDL
df_ddl_schema = spark_session.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")
df_ddl_schema.createOrReplaceTempView("divvy_trips")
df_ddl_with_schema = spark_session.sql("SELECT * FROM divvy_trips")
print("\nSchema via DDL:")
df_ddl_with_schema.printSchema()
print("Number of records:", df_ddl_with_schema.count())

# Selecting Gender and filtering
df_filtered = df_ddl_schema.select("gender").filter((df_ddl_schema["gender"] == "Female") | (df_ddl_schema["gender"] == "Male"))
print("\nFiltered DataFrame:")
df_filtered.show(10)

# Select gender based on last name and group by station
selected_gender = df_ddl_with_schema.selectExpr("CASE WHEN substring(gender, 1, 1) >= 'A' AND substring(gender, 1, 1) <= 'K' THEN 'Female' ELSE 'Male' END AS selected_gender", "to_station_name").groupBy("to_station_name").count()
selected_gender.show(10)

# Stop the SparkSession
spark_session.stop()

#update1