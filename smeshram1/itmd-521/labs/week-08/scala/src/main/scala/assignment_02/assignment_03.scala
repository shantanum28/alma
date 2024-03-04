import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, DateType, IntegerType, StringType}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat

object assignment_03 {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03.scala <file_path>")
      System.exit(1)
    }

    val spark = SparkSession.builder
      .appName("assignment_03")
      .getOrCreate()

    try {
      val inputFilePath = args(0)

      // Read CSV and define schema
      val schema = StructType(Seq(
        StructField("date", StringType, true),
        StructField("delay", IntegerType, true),
        StructField("distance", IntegerType, true),
        StructField("origin", StringType, true),
        StructField("destination", StringType, true)
      ))

      val df = spark.read
        .option("header", "true")
        .schema(schema)
        .csv(inputFilePath)

      // Cast date column to timestamp
      val dateFormat = new SimpleDateFormat("MMddHHmm")
      val castDateUDF = udf((date: String) => new java.sql.Timestamp(dateFormat.parse(date).getTime))
      val dfWithTimestamp = df.withColumn("date", castDateUDF(col("date")))

      // Identify common delays
      val winterMonthExpr = (month(col("date")) >= 12) || (month(col("date")) <= 2)
      val holidayExpr = dayofweek(col("date")).isin(1, 7)

      val df1 = dfWithTimestamp
        .withColumn("Winter_Month", when(winterMonthExpr, "Yes").otherwise("No"))
        .withColumn("Holiday", when(holidayExpr, "Yes").otherwise("No"))
        .groupBy(date_format("date", "MM-dd").alias("month_day"), "Winter_Month", "Holiday")
        .agg(count("*").alias("count"))
        .orderBy(desc("count"))
        .limit(10)

      df1.show()

      // Label delay categories
      val delayExpr = col("delay")
      val df2 = dfWithTimestamp
        .withColumn("Flight_Delays",
          when(delayExpr > 360, "Very Long Delays")
            .when((delayExpr >= 120) && (delayExpr < 360), "Long Delays")
            .when((delayExpr >= 60) && (delayExpr < 120), "Short Delays")
            .when((delayExpr > 0) && (delayExpr < 60), "Tolerable Delays")
            .when(delayExpr === 0, "No Delays")
            .otherwise("Early"))

      df2.select("delay", "origin", "destination", "Flight_Delays").limit(10).show()

      // Create temporary table
      dfWithTimestamp.createOrReplaceTempView("us_delay_flights_tbl")

      // Extract month and day
      val dfWithMonthAndDay = dfWithTimestamp
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))

      // Filter dataframe
      val filteredDF = dfWithMonthAndDay
        .filter((col("origin") === "ORD") && (month("date") === 3) && (dayofmonth("date").between(1, 15)))

      filteredDF.show()

      // List table columns
      val columns = spark.catalog.listColumns("us_delay_flights_tbl")
      println("Columns of us_delay_flights_tbl:")
      columns.show()

      // Write data to JSON, LZ4 JSON, and Parquet
      dfWithTimestamp.write.mode("overwrite").json("departuredelays.json")
      dfWithTimestamp.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")
      dfWithTimestamp.write.mode("overwrite").parquet("departuredelays.parquet")

      // Read Parquet file and filter ORD records
      val parquetDF = spark.read.parquet("departuredelays.parquet")
      val ordDepartures = parquetDF.filter(col("origin") === "ORD")
      ordDepartures.write.mode("overwrite").parquet("orddeparturedelays.parquet")
      ordDepartures.show(10)

    } finally {
      spark.stop()
    }
  }
}
