import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, month, dayofyear, dayofmonth

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit assignment_03.py <file_path>", file=sys.stderr)
        sys.exit(-1)

    file_path = sys.argv[1]

    # Initialize Spark session
    spark = SparkSession.builder.appName("Assignment03").getOrCreate()

    # Part I - Query 1
    df = spark.read.format("csv").option("header", "true").load(file_path)
    df = df.withColumn("date", expr("to_date(date, 'MMddHHmm')"))
    df.createOrReplaceTempView("flights")

    # SQL Query
    query_1_sql = """
    SELECT
        date,
        delay,
        distance,
        origin,
        destination,
        CASE WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter' ELSE 'Not Winter' END as winter_month,
        CASE WHEN dayofyear(date) IN (1, 25, 122, 245) THEN 'Holiday' ELSE 'Not Holiday' END as holiday
    FROM flights
    WHERE delay > 0
    ORDER BY delay DESC
    LIMIT 10
    """
    result_query_1_sql = spark.sql(query_1_sql)
    result_query_1_sql.show()

    # DataFrame API Query
    result_query_1_api = (
        df.filter(col("delay") > 0)
        .withColumn("winter_month", when(month("date").isin(12, 1, 2), "Winter").otherwise("Not Winter"))
        .withColumn("holiday", when(dayofyear("date").isin(1, 25, 122, 245), "Holiday").otherwise("Not Holiday"))
        .orderBy(col("delay").desc())
        .limit(10)
    )
    result_query_1_api.show()

    # Part I - Query 2
    query_2_sql = """
    SELECT
        *,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END as Flight_Delays
    FROM flights
    """
    result_query_2_sql = spark.sql(query_2_sql)
    result_query_2_sql.show(10)

    # DataFrame API Query
    result_query_2_api = (
        df.withColumn(
            "Flight_Delays",
            when(col("delay") > 360, "Very Long Delays")
            .when((col("delay") > 120) & (col("delay") < 360), "Long Delays")
            .when((col("delay") > 60) & (col("delay") < 120), "Short Delays")
            .when((col("delay") > 0) & (col("delay") < 60), "Tolerable Delays")
            .when(col("delay") == 0, "No Delays")
            .otherwise("Early")
        )
    )
    result_query_2_api.show(10)

    # Part II
    df.createOrReplaceTempView("us_delay_flights_tbl")

    # Use Spark Catalog to list columns of table
    catalog = spark.catalog

    # Get the columns of the table 'us_delay_flights_tbl'
    table_columns = catalog.listColumns("us_delay_flights_tbl")

    # Display the columns
    print("Columns of us_delay_flights_tbl:")
    for column in table_columns:
        print(column.name)
    
    # SQL Query
    query_part_ii_sql = """
    SELECT *
    FROM us_delay_flights_tbl
    WHERE origin = 'ORD' AND month(date) = 3 AND day(date) BETWEEN 1 AND 15
    LIMIT 5
    """
    result_part_ii_sql = spark.sql(query_part_ii_sql)
    result_part_ii_sql.show()

    # DataFrame API Query
    result_part_ii_api = (
        df.filter((col("origin") == "ORD") & (month("date") == 3) & (dayofmonth("date").between(1, 15)))
        .limit(5)
    )
    result_part_ii_api.show()

    # Part III
    df = df.withColumn("date", col("date").cast("date"))

    # Write as JSON
    df.write.mode("overwrite").json("departuredelays.json")
    # Write as JSON with lz4 compression
    df.write.mode("overwrite").format("json").option("compression", "lz4").save("departuredelays_lz4.json")
    # Write as Parquet
    df.write.mode("overwrite").parquet("departuredelays.parquet")

    # Part IV
    parquet_df = spark.read.parquet("departuredelays.parquet")

    # SQL Query
    query_part_iv_sql = """
    SELECT *
    FROM __THIS__
    WHERE origin = 'ORD'
    """
    parquet_df.createOrReplaceTempView("__THIS__")
    result_part_iv_sql = spark.sql(query_part_iv_sql)
    result_part_iv_sql.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    result_part_iv_sql.show(10)

    # DataFrame API Query
    ord_departure_delays_api = parquet_df.filter(col("origin") == "ORD")
    ord_departure_delays_api.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    ord_departure_delays_api.show(10)

    spark.stop()
