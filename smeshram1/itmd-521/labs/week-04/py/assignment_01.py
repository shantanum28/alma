import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def main():
    if len(sys.argv) != 2:
        print("Usage: DivvyTrips <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("Assignment 1").getOrCreate()
    data_source = sys.argv[1]

    # Inferring Schema
    infer_divvy_df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_source)
    print_data_frame_info("Inferring Schema:", infer_divvy_df, 10)

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
    struct_divvy_df = spark.read.schema(struct_schema).option("header", "true").csv(data_source)
    print_data_frame_info("\nProgrammatically Defined Schema:", struct_divvy_df, 10)

    # Attaching a schema via DDL
    ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    ddl_df = spark.read.option("header", "true").schema(ddl_schema).csv(data_source)
    print_data_frame_info("\nSchema via DDL:", ddl_df, 10)

    # Selecting Gender and Grouping by station to name
    select_gender_df = infer_divvy_df.select("gender", "to_station_name") \
        .filter((infer_divvy_df["gender"] == "Female") | (infer_divvy_df["gender"] == "Female")) \
        .groupBy("to_station_name").count()

    print_data_frame_info("\nFiltered by gender and grouped by station to:", select_gender_df, 10)

    spark.stop()

def print_data_frame_info(title, df, num_rows=None):
    print(title)
    df.show(truncate=False, n=num_rows)
    df.printSchema()
    print(f"Number of records: {df.count()}\n")

if __name__ == "__main__":
    main()