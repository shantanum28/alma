import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("My Spark Assignment - Python") \
    .getOrCreate()

# Path to the CSV file
csv_file_path = sys.argv[1]

# First DataFrame: Infer Schema
df_infer_schema = spark.read.csv(csv_file_path, header=True, inferSchema=True)
print("DataFrame with Inferred Schema:")
df_infer_schema.printSchema()
print("Total Records:", df_infer_schema.count())

# Second DataFrame: Programmatically Define Schema
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", StringType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", StringType(), True)
])
df_programmatic_schema = spark.read.csv(csv_file_path, header=True, schema=schema)
print("DataFrame with Programmatically Defined Schema:")
df_programmatic_schema.printSchema()
print("Total Records:", df_programmatic_schema.count())

# Third DataFrame: Attach Schema via DDL
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration STRING, from_station_name STRING, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"
df_ddl_schema = spark.read.csv(csv_file_path, header=True, schema=ddl_schema)
print("DataFrame with Schema Attached via DDL:")
df_ddl_schema.printSchema()
print("Total Records:", df_ddl_schema.count())

# Select Gender based on last name
last_name = ""
gender = "female" if last_name.lower() <= "k" else "male"
print("Selected Gender based on last name:", gender)

# GroupBy station_name and show 10 records
df_grouped = df_infer_schema.groupBy("to_station_name").count()
print("Grouped DataFrame:")
df_grouped.show(10)

# Stop the SparkSession
spark.stop()