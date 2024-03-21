from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment_01.py <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("assignment_01").getOrCreate()

    file_path = sys.argv[1]

    # Inferred schema
    df1 = spark.read.option("header", True).csv(file_path)
    print("DataFrame 1 - Inferred Schema:")
    df1.printSchema()
    print("Number of Records:", df1.count())

    # Programmatically defined schema
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
        StructField("birthyear", IntegerType(), True),
    ])

    df2 = spark.read.option("header", True).schema(schema).csv(file_path)
    print("\nDataFrame 2 - Programmatically Defined Schema:")
    df2.printSchema()
    print("Number of Records:", df2.count())

    # DDL-defined schema
    ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, " \
                "from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, " \
                "usertype STRING, gender STRING, birthyear INT"

    df3 = spark.read.option("header", True).option("inferSchema", False).schema(ddl_schema).csv(file_path)
    print("\nDataFrame 3 - DDL Defined Schema:")
    df3.printSchema()
    print("Number of Records:", df3.count())

    last_name = "meshram"
    last_name_starts_with_a_to_k = "A" <= last_name[0].upper() <= "K"
    selected_gender = "Female" if last_name_starts_with_a_to_k else "Male"
    print(f"\nSelected Gender: {selected_gender}")

    filtered_df = df3.filter(df3["gender"] == selected_gender)

    grouped_df = filtered_df.groupBy("to_station_name").count()

    print("\nGroupBy to_station_name and Display 10 Records:")
    grouped_df.limit(10).show(truncate=False)

    spark.stop()
