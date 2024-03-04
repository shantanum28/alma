from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType
from pyspark.sql.functions import col, date_format, when, month, dayofweek, dayofmonth, to_date

def initialize_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv_to_dataframe(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)

def define_dataframe_schema():
    return StructType([
        StructField("date", StringType(), True),
        StructField("delay", IntegerType(), True),
        StructField("distance", IntegerType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True)
    ])

def cast_date_column_to_timestamp(df):
    return df.withColumn("date", col("date").cast("TIMESTAMP"))

def identify_common_delays(df):
    winter_month_expr = (month(col("date")) >= 12) | (month(col("date")) <= 2)
    holiday_expr = dayofweek(col("date")).isin([1, 7])

    return (
        df.withColumn("Winter_Month", when(winter_month_expr, "Yes").otherwise("No"))
          .withColumn("Holiday", when(holiday_expr, "Yes").otherwise("No"))
          .groupBy(date_format("date", "MM-dd").alias("month_day"), "Winter_Month", "Holiday")
          .count()
          .orderBy(col("count").desc())
    )

def label_delay_categories(df):
    delay_expr = col("delay")
    return df.withColumn("Flight_Delays", 
                       when(delay_expr > 360, "Very Long Delays")
                       .when((delay_expr >= 120) & (delay_expr < 360), "Long Delays")
                       .when((delay_expr >= 60) & (delay_expr < 120), "Short Delays")
                       .when((delay_expr > 0) & (delay_expr < 60), "Tolerable Delays")
                       .when(delay_expr == 0, "No Delays")
                       .otherwise("Early"))

def create_temporary_table(df):
    df.createOrReplaceTempView("us_delay_flights_tbl")

def extract_month_and_day(df):
    df = df.withColumn("month", month("date")).withColumn("day", dayofmonth("date"))
    return df

def filter_dataframe(df):
    return df.filter(
        (col("origin") == "ORD") & 
        (month("date") == 3) & 
        (dayofmonth("date").between(1, 15))
    )

def show_dataframe(df, num_records=5):
    df.show(num_records)

def list_table_columns(spark, table_name):
    table_columns = spark.catalog.listColumns(table_name)
    print("Columns of", table_name + ":")
    for column in table_columns:
        print(column.name)

def write_dataframe_to_json(df, output_path):
    df.write.mode("overwrite").json(output_path)

def write_dataframe_to_lz4_json(df, output_path):
    df.write.mode("overwrite").format("json").option("compression", "lz4").save(output_path)

def write_dataframe_to_parquet(df, output_path):
    df.write.mode("overwrite").parquet(output_path)

def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit assignment_03.py <file_path>", file=sys.stderr)
        sys.exit(-1)

    spark = initialize_spark_session("assignment_03")
    input_file_path = sys.argv[1]

    df = read_csv_to_dataframe(spark, input_file_path)
    schema = define_dataframe_schema()
    df = cast_date_column_to_timestamp(df)
    df1 = identify_common_delays(df)
    df1.show(10)

    df2 = label_delay_categories(df)
    df2.select("delay", "origin", "destination", "Flight_Delays").show(10)

    create_temporary_table(df)

    df = cast_date_column_to_timestamp(df)
    df = extract_month_and_day(df)

    filtered_df = filter_dataframe(df)
    show_dataframe(filtered_df)

    list_table_columns(spark, "us_delay_flights_tbl")

    json_output_path = "departuredelays.json"
    write_dataframe_to_json(df, json_output_path)

    lz4_json_output_path = "departuredelays_lz4.json"
    write_dataframe_to_lz4_json(df, lz4_json_output_path)

    parquet_output_path = "departuredelays.parquet"
    write_dataframe_to_parquet(df, parquet_output_path)

    print("Data has been written to the specified files.")

    parquet_file_path = "departuredelays.parquet"
    df = spark.read.parquet(parquet_file_path)
    df = df.withColumn("date", to_date(df["date"], "MMddHHmm"))

    ord_df = df.filter(df.origin == "ORD")

    ord_parquet_output_path = "orddeparturedelays.parquet"
    ord_df.write.mode("overwrite").parquet(ord_parquet_output_path)

    ord_df.show(10)

if __name__ == "__main__":
    main()
