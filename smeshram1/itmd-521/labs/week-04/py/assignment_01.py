import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, substring

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment-01 <file>", file=sys.stderr)
        sys.exit(-1)
# Create SparkSession
spark = (SparkSession
    .builder 
    .appName("PythonDivvyTripsAnalysis") 
    .getOrCreate())
# Get the Divvy Trips CSV file path from command-line argument
divvy_file = sys.argv[1]
# Define schema for DataFrame creation
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", DoubleType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])
# Read CSV file with schema inference
df_infer = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(divvy_file))
df_infer.printSchema()
print("Number of records:", df_infer.count())
# Read CSV file with specified schema
df_structfields = (spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load(divvy_file))
df_structfields.printSchema()
print("Number of records:", df_structfields.count())
# Read CSV file with schema defined via DDL
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration DOUBLE,from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING,usertype STRING, gender STRING, birthyear INT"
df_ddl = (spark.read.format("csv")
    .option("header", "true")
    .schema(ddl_schema)
    .load(divvy_file))
df_ddl.printSchema()
#Select gender and to_station_name
print("Number of records:", df_ddl.count())
selected_df = df_ddl.select("gender", "to_station_name") .filter(col("gender") == "Male") .groupBy("to_station_name") .count()

# Show 10 records of the DataFrame
selected_df.show(10,truncate=False)

# Stop SparkSession
spark.stop()