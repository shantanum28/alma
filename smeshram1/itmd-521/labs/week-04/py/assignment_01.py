from os import truncate
import sys
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: DivvyTrips <file>", file=sys.stderr)
        sys.exit(-1)

    # Create SparkSession
    spark = SparkSession.builder.appName("Assignment 1").getOrCreate()
    data_source = sys.argv[1]

    # Inferring Schema
    infer_divvy_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_source)
    filtered_df = infer_divvy_df.where(col("gender") == "Male")
    grouped_df = filtered_df.groupBy("to_station_name").count()
    grouped_df.show(10)
    print("Number of records: ", infer_divvy_df.count())
    print("Schema Inferred Data Frame:")
    # Print the schema
    infer_divvy_df.printSchema()


    # Programmatically creating and attaching a schema using StructFields
    struct_schema = StructType([
        StructField("trip_id", IntegerType()),
        StructField("starttime", StringType()),
        StructField("stoptime", StringType()),
        StructField("bikeid", IntegerType()),
        StructField("tripduration", IntegerType()),
        StructField("from_station_id", IntegerType()),
        StructField("from_station_name", StringType()),
        StructField("to_station_id", IntegerType()),
        StructField("to_station_name", StringType()),
        StructField("usertype", StringType()),
        StructField("gender", StringType()),
        StructField("birthyear", IntegerType())
    ])
    #Programmatically
    struct_divvy_df = spark.read.schema(struct_schema).format("csv").option("header", "true").load(data_source)
    female_df = struct_divvy_df.where(col("gender") == "Male")
    fegrouped_df = female_df.groupBy("to_station_name").count()
    fegrouped_df.show(10)
    print("Number of records:", struct_divvy_df.count())
    print("Programmatically Defined Schema:")
    struct_divvy_df.printSchema()
    

    #ddl
    ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    ddl_df = spark.read.schema(ddl_schema).format("csv").option("header", "true").load(data_source)
    ddfemale_df = ddl_df.where(col("gender") == "Male")
    ddlgrouped_df = ddfemale_df.groupBy("to_station_name").count()
    ddlgrouped_df.show(10)
    print("Number of records:", ddl_df.count())
    print("\nSchema via DDL:")
    ddl_df.printSchema()

    # Stoping SparkSession
    spark.stop()