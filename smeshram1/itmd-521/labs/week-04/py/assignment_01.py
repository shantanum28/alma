import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: DivvyTrips <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("Assignment 1").getOrCreate()
    data_source = sys.argv[1]

    # Read data with inferred schema
    divvy_df = spark.read.options(header='true', inferSchema='true').csv(data_source)
    female_stations_count = divvy_df.filter(divvy_df.gender == "Female") \
                                    .groupBy("to_station_name") \
                                    .count()
    female_stations_count.show(10)
    print("Number of records (inferred schema): ", divvy_df.count())
    divvy_df.printSchema()

    # Read data with programmatically defined schema
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

    divvy_df_with_schema = spark.read.format("csv") \
                                      .option("header", "true") \
                                      .schema(struct_schema) \
                                      .load(data_source)
    female_stations_count_with_schema = divvy_df_with_schema.filter(divvy_df_with_schema.gender == "Female") \
                                                             .groupBy("to_station_name") \
                                                             .count()
    female_stations_count_with_schema.show(10)
    print("Number of records (programmatically defined schema):", divvy_df_with_schema.count())
    divvy_df_with_schema.printSchema()

    # Read data with DDL string schema
    ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    divvy_df_with_ddl_schema = spark.read.format("csv") \
                                         .option("header", "true") \
                                         .schema(ddl_schema) \
                                         .load(data_source)
    female_stations_count_with_ddl_schema = divvy_df_with_ddl_schema.filter(divvy_df_with_ddl_schema.gender == "Female") \
                                                                     .groupBy("to_station_name") \
                                                                     .count()
    female_stations_count_with_ddl_schema.show(10)
    print("Number of records (DDL schema):", divvy_df_with_ddl_schema.count())
    divvy_df_with_ddl_schema.printSchema()

    spark.stop()