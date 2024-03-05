import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr, when, month, dayofyear, dayofmonth}

object assignment_03 {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      Console.err.println("Usage: spark-submit assignment_03 <file_path>")
      System.exit(-1)
    }

    val filePath = args(0)

    // Initialize Spark session
    val spark = SparkSession.builder.appName("assignment_03").getOrCreate()

    // Part I - Query 1
    var df = spark.read.format("csv").option("header", "true").load(filePath)
    df = df.withColumn("date", expr("to_date(date, 'MMddHHmm')"))
    df.createOrReplaceTempView("flights")

    // SQL Query
    val query1Sql =
      """
        |SELECT
        |    date,
        |    delay,
        |    distance,
        |    origin,
        |    destination,
        |    CASE WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter' ELSE 'Not Winter' END as winter_month,
        |    CASE WHEN dayofyear(date) IN (1, 25, 122, 245) THEN 'Holiday' ELSE 'Not Holiday' END as holiday
        |FROM flights
        |WHERE delay > 0
        |ORDER BY delay DESC
        |LIMIT 10
      """.stripMargin
    val resultQuery1Sql = spark.sql(query1Sql)
    resultQuery1Sql.show()

    // DataFrame API Query
    val resultQuery1Api =
      df.filter(col("delay") > 0)
        .withColumn("winter_month", when(month("date").isin(12, 1, 2), "Winter").otherwise("Not Winter"))
        .withColumn("holiday", when(dayofyear("date").isin(1, 25, 122, 245), "Holiday").otherwise("Not Holiday"))
        .orderBy(col("delay").desc())
        .limit(10)
    resultQuery1Api.show()

    // Part I - Query 2
    val query2Sql =
      """
        |SELECT
        |    *,
        |    CASE
        |        WHEN delay > 360 THEN 'Very Long Delays'
        |        WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        |        WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        |        WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
        |        WHEN delay = 0 THEN 'No Delays'
        |        ELSE 'Early'
        |    END as Flight_Delays
        |FROM flights
      """.stripMargin
    val resultQuery2Sql = spark.sql(query2Sql)
    resultQuery2Sql.show(10)

    // DataFrame API Query
    val resultQuery2Api =
      df.withColumn(
        "Flight_Delays",
        when(col("delay") > 360, "Very Long Delays")
          .when((col("delay") > 120) && (col("delay") < 360), "Long Delays")
          .when((col("delay") > 60) && (col("delay") < 120), "Short Delays")
          .when((col("delay") > 0) && (col("delay") < 60), "Tolerable Delays")
          .when(col("delay") === 0, "No Delays")
          .otherwise("Early")
      )
    resultQuery2Api.show(10)

    // Part II
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // Use Spark Catalog to list columns of table
    val catalog = spark.catalog

    // Get the columns of the table 'us_delay_flights_tbl'
    val tableColumns = catalog.listColumns("us_delay_flights_tbl")

    // Display the columns
    println("Columns of us_delay_flights_tbl:")
    tableColumns.foreach(column => println(column.name))

    // SQL Query
    val queryPartIISql =
      """
        |SELECT *
        |FROM us_delay_flights_tbl
        |WHERE origin = 'ORD' AND month(date) = 3 AND day(date) BETWEEN 1 AND 15
        |LIMIT 5
      """.stripMargin
    val resultPartIISql = spark.sql(queryPartIISql)
    resultPartIISql.show()

    // DataFrame API Query
    val resultPartIIApi =
      df.filter((col("origin") === "ORD") && (month("date") === 3) && (dayofmonth("date").between(1, 15)))
        .limit(5)
    resultPartIIApi.show()

    // Part III
    df = df.withColumn("date", col("date").cast("date"))

    // Write as JSON
    df.write.mode("overwrite").json("departuredelays.json")
    // Write as JSON with lz4 compression
    df.write.mode("overwrite").format("json").option("compression", "lz4").save("departuredelays_lz4.json")
    // Write as Parquet
    df.write.mode("overwrite").parquet("departuredelays.parquet")

    // Part IV
    val parquetDf = spark.read.parquet("departuredelays.parquet")

    // SQL Query
    val queryPartIVSql =
      """
        |SELECT *
        |FROM __THIS__
        |WHERE origin = 'ORD'
      """.stripMargin
    parquetDf.createOrReplaceTempView("__THIS__")
    val resultPartIVSql = spark.sql(queryPartIVSql)
    resultPartIVSql.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    resultPartIVSql.show(10)

    // DataFrame API Query
    val ordDepartureDelaysApi = parquetDf.filter(col("origin") === "ORD")
    ordDepartureDelaysApi.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    ordDepartureDelaysApi.show(10)

    spark.stop()
  }
}
