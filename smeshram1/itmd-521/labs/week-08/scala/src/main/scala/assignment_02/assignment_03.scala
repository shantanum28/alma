import org.apache.spark.sql.{SparkSession, functions}

object Assignment03SparkAPI {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.exit(-1)
    }

    val filePath = args(0)

    // Initialize Spark session
    val spark = SparkSession.builder.appName("Assignment03SparkAPI").getOrCreate()

    // Part I - Query 1
    var df = spark.read.format("csv").option("header", "true").load(filePath)
    df = df.withColumn("date", functions.expr("to_date(date, 'MMddHHmm')"))
    df.createOrReplaceTempView("flights")

    // Spark API Query 1
    val resultQuery1API = df
      .filter("delay > 0")
      .withColumn("winter_month", functions.when(functions.month(df("date")).isin(12, 1, 2), "Winter").otherwise("Not Winter"))
      .withColumn("holiday", functions.when(functions.dayofyear(df("date")).isin(1, 25, 122, 245), "Holiday").otherwise("Not Holiday"))
      .select("date", "delay", "distance", "origin", "destination", "winter_month", "holiday")
      .orderBy(functions.col("delay").desc)
      .limit(10)

    resultQuery1API.show()

    // Part I - Query 2
    val resultQuery2API = df
      .withColumn("Flight_Delays",
        functions.when(functions.col("delay") > 360, "Very Long Delays")
          .when(functions.col("delay") > 120 && functions.col("delay") < 360, "Long Delays")
          .when(functions.col("delay") > 60 && functions.col("delay") < 120, "Short Delays")
          .when(functions.col("delay") > 0 && functions.col("delay") < 60, "Tolerable Delays")
          .when(functions.col("delay") === 0, "No Delays")
          .otherwise("Early")
      )

    resultQuery2API.show(10)

    // Part II
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // Spark API Part II
    val resultPartIIAPI = df
      .filter("origin = 'ORD' AND month(date) = 3 AND day(date) BETWEEN 1 AND 15")
      .limit(5)

    resultPartIIAPI.show()

    // Use Spark Catalog to list column names of us_delay_flights_tbl
    val tableColumns = spark.catalog.listColumns("us_delay_flights_tbl")
    val columnNames = tableColumns.select("name")
    println("Columns of us_delay_flights_tbl:")
    columnNames.show(false)

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

    // Spark API Part IV
    parquetDF.createOrReplaceTempView("__THIS__")
    val resultPartIVAPI = spark.sql("SELECT * FROM __THIS__ WHERE origin = 'ORD'")
    resultPartIVAPI.write.mode("overwrite").parquet("orddeparturedelays.parquet")
    resultPartIVAPI.show(10)

    spark.stop()
  }
}
