from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Divvy Trips") \
    .getOrCreate()

# Define schema programmatically
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

# Read CSV with inferred schema
df_inferred = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, inferSchema=True)
print("Inferred Schema:")
df_inferred.printSchema()
print("Number of records:", df_inferred.count())

# Read CSV with programmatically defined schema
df_programmatic = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, schema=schema)
print("\nProgrammatic Schema:")
df_programmatic.printSchema()
print("Number of records:", df_programmatic.count())

# Read CSV with schema via DDL
df_ddl = spark.read.option("header", "true").schema(schema).csv("Divvy_Trips_2015-Q1.csv")
print("\nSchema via DDL:")
df_ddl.printSchema()
print("Number of records:", df_ddl.count())

spark.stop()