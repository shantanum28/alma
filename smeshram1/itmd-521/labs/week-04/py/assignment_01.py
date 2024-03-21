import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment-01 <file>", file=sys.stderr)
        sys.exit(-1)

# Creating Spark session
    spark = (SparkSession
      .builder
      .appName("DivvyTrips")
      .getOrCreate())

# Defining file path
    divvy_file= sys.argv[1]

# Creating DataFrame schema 
    divvy_schema = StructType([
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

# Read the CSV file using infer and printing schema and count for each DataFrame
    divvy_df_infer = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(divvy_file))
    divvy_df_infer.printSchema()
    print("Number of records in python- infer schema:", divvy_df_infer.count())

# Read the CSV file using structfield and printing schema and count for each DataFrame
    divvy_df_struct_fields = (spark.read.format("csv")
      .option("header", "true")
      .option("schema", "divvy_schema")
      .load(divvy_file))
    divvy_df_struct_fields.printSchema()
    print("Number of records in python- StructFields schema:", divvy_df_struct_fields.count())

# DDL Schema
    divvy_ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    divvy_df_ddl = (spark.read.format("csv")
      .option("header", "true")
      .option("schema", "divvy_ddl_schema")
      .load(divvy_file))
    divvy_df_ddl.printSchema()
    print("Number of records in python- DDL schema:", divvy_df_ddl.count())

# Select statement for gender, to_station_name grouping and counting
    divvy_selected_df = divvy_df_ddl.select("to_station_name", "gender").where(col("gender") == "Male").groupBy("to_station_name").count()

# Show 10 records of the DataFrame
    divvy_selected_df.show(10, truncate=False)

# Stop spark
spark.stop()