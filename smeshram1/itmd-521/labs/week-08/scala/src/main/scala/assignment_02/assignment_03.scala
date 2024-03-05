import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._

object assignment_03 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.exit(-1)
    }

    val filePath = args(0)

    // Initialize Spark session
    val spark = SparkSession.builder.appName("assignment_03").getOrCreate()

    // Part I - Query 1
    var df = spark.read.format("csv").option("header", "true").load(filePath)
    df = df.withColumn("date", functions.expr("to_date(date, 'MMddHHmm')"))
    df.createOrReplaceTempView("flights")

    // SQL Query
    val query1SQL =
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
    val resultQuery1SQL = spark.sql(query1SQL)
    resultQuery1SQL.show()

    // Corresponding DataFrame API
    import spark.implicits._
    val resultQuery1DF = df
      .filter("delay > 0")
      .withColumn("winter_month", functions.when(functions.month("date").isin(12, 1, 2), "Winter").otherwise("Not Winter"))
      .withColumn("holiday", functions.when(functions.dayofyear("date").isin(1, 25, 122, 245), "Holiday").otherwise("Not Holiday"))
      .select("date", "delay", "distance", "origin", "destination", "winter_month", "holiday")
      .orderBy(functions.col("delay").desc)
      .limit(10)
    resultQuery1DF.show()

    // Part I - Query 2
    val query2SQL =
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
    val resultQuery2SQL = spark.sql(query2SQL)
    resultQuery2SQL.show(10)

    // Corresponding DataFrame API
    val resultQuery2DF = df
      .withColumn("Flight_Delays", functions.when(functions.col("delay") > 360, "Very Long Delays")
        .when((functions.col("delay") > 120) && (functions.col("delay") < 360), "Long Delays")
        .when((functions.col("delay") > 60) && (functions.col("delay") < 120), "Short Delays")
        .when((functions.col("delay") > 0) && (functions.col("delay") < 60), "Tolerable Delays")
        .when(functions.col("delay") === 0, "No Delays")
        .otherwise("Early"))
    resultQuery2DF.show(10)

    // Part II
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // SQL Query
    val queryPartIISQL =
      """
        |SELECT *
        |FROM us_delay_flights_tbl
        |WHERE origin = 'ORD' AND month(date) = 3 AND day(date) BETWEEN 1 AND 15
        |LIMIT 5
      """.stripMargin
    val resultPartIISQL = spark.sql(queryPartIISQL)
    resultPartIISQL.show()

    // Corresponding DataFrame API
    val resultPartIIDF = df
      .filter("origin = 'ORD' AND month(date) = 3 AND day(date) BETWEEN 1 AND 15")
      .limit(5)
    resultPartIIDF.show()

    // Part III
    df = df.withColumn("date", functions.col("date").cast("date"))

    // Write as JSON
    df.write.mode("overwrite").json("departuredelays.json")
    // Write as JSON with lz4 compression
    df.write.mode("overwrite").format("json").option("compression", "lz4").save("departuredelays_lz4.json")
    // Write as Parquet
    df.write.mode("overwrite").parquet("departuredelays.parquet")

    // Part IV
    val parquetDF = spark.read.parquet("departuredelays.parquet")

    // SQL Query
    val queryPartIVSQL =
      """
        |SELECT *
        |FROM __THIS__
        |WHERE origin = 'ORD'
      """.stripMargin
    parquetDF.createOrReplaceTempView("__THIS__")
    val resultPartIVSQL = spark.sql(queryPartIVSQL)
    resultPartIVSQL.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    resultPartIVSQL.show(10)

    spark.stop()
  }
}
