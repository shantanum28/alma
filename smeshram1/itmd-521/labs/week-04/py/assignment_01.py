# Importing Sparksession and related functions
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import count, col

if __name__ == "__main__":
   if len(sys.argv) != 2:
      print("Usage: Divvy_Trips_2015-Q1 <path to file>", file=sys.stderr)
      sys.exit(-1)
    
# Building a SparkSession
spark = (SparkSession
         .builder
         .appName("Divvy Trips Analysis")
         .getOrCreate())  

# Get Divvy Trips data set filename
divvy_data_file = sys.argv[1]

# Reading datafile, inferring schema and printing the count
print("Inferred printSchema for Divvy Trips")
trips_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(divvy_data_file))

trips_df.printSchema()
print("Total number of Divvy trips:", trips_df.count())

# Reading datafile programmatically and printing the count
print("Programmatic printSchema for Divvy Trips")
struct_divvy_schema = StructType([
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
   StructField("birthyear", IntegerType(), True),])

prog_trips_df = (spark.read.format("csv")
                 .option("header", "true")
                 .schema(struct_divvy_schema)
                 .load(divvy_data_file))

prog_trips_df.printSchema()
print("Total number of Divvy trips:", prog_trips_df.count())

# Reading datafile using DDL and printing the count
print("DDL printSchema for Divvy Trips")
ddl_divvy_schema = "trip_id INT, starttime STRING, stoptime STRING,\
                   bikeid INT, tripduration INT, from_station_id INT,\
                   from_station_name STRING, to_station_id INT,\
                   to_station_name STRING, usertype STRING, gender STRING,\
                   birthyear INT"

ddl_trips_df = (spark.read.format("csv")
                .option("header", "true")
                .schema(ddl_divvy_schema)
                .load(divvy_data_file))

ddl_trips_df.printSchema()
print("Total number of Divvy trips:", ddl_trips_df.count())

# Displaying 10 results for female grouping by to_station_name
# Tranformation and actions
fem_trips_df = (trips_df.select("*")
               .where(col("gender") == "Female")
               .groupBy("to_station_name")
               .count())
fem_trips_df.show(10, truncate= False)

# Stop sparkSession
spark.stop()