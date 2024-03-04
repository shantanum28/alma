import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_03 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03.scala <file_path>")
      System.exit(1)
    }

    val filePath = args(0)
    val spark = SparkSession.builder().appName("assignment_03").getOrCreate()

    // Reading CSV to DataFrame
    val schema = StructType(Array(
      StructField("date", StringType, true),
      StructField("delay", IntegerType, true),
      StructField("distance", IntegerType, true),
      StructField("origin", StringType, true),
      StructField("destination", StringType, true)
    ))

    var df = spark.read.option("header", true).schema(schema).csv(filePath)

    // Casting date column to timestamp
    df = df.withColumn("date", col("date").cast("timestamp"))

    // Identifying common delays
    val winterMonthExpr = (month(col("date")) >= 12) || (month(col("date")) <= 2)
    val holidayExpr = dayofweek(col("date")).isin(1, 7)

    val commonDelaysDF = df
      .withColumn("Winter_Month", when(winterMonthExpr, "Yes").otherwise("No"))
      .withColumn("Holiday", when(holidayExpr, "Yes").otherwise("No"))
      .groupBy(date_format("date", "MM-dd").alias("month_day"), "Winter_Month", "Holiday")
      .count()
      .orderBy(col("count").desc())

    commonDelaysDF.show(10)

    // Labeling delay categories
    val labeledDF = df.withColumn("Flight_Delays", expr(
      """CASE
        | WHEN delay > 360 THEN 'Very Long Delays'
        | WHEN delay >= 120 AND delay < 360 THEN 'Long Delays'
        | WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
        | WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
        | WHEN delay = 0 THEN 'No Delays'
        | ELSE 'Early' END""".stripMargin))

    labeledDF.select("delay", "origin", "destination", "Flight_Delays").show(10)

    // Creating temporary table
    df.createOrReplaceTempView("us_delay_flights_tbl")

    // Extracting month and day
    df = df.withColumn("month", month("date")).withColumn("day", dayofmonth("date"))

    // Filtering DataFrame
    val filteredDF = df.filter(col("origin") === "ORD" && month("date") === 3 && dayofmonth("date").between(1, 15))
    filteredDF.show(5)

    // Listing table columns
    spark.catalog.listColumns("us_delay_flights_tbl").show()

    // Writing DataFrame to JSON
    val jsonOutputPath = "departuredelays.json"
    df.write.mode(SaveMode.Overwrite).json(jsonOutputPath)

    // Writing DataFrame to LZ4 JSON
    val lz4JsonOutputPath = "departuredelays_lz4.json"
    df.write.mode(SaveMode.Overwrite).option("compression", "lz4").json(lz4JsonOutputPath)

    // Writing DataFrame to Parquet
    val parquetOutputPath = "departuredelays.parquet"
    df.write.mode(SaveMode.Overwrite).parquet(parquetOutputPath)

    println("Data has been written to the specified files.")

    // Reading Parquet file
    val parquetFilePath = "departuredelays.parquet"
    df = spark.read.parquet(parquetFilePath)
    df = df.withColumn("date", to_date(col("date"), "MMddHHmm"))

    // Selecting records with ORD as origin
    val ordDF = df.filter(col("origin") === "ORD")
    ordDF.write.mode(SaveMode.Overwrite).parquet("orddeparturedelays.parquet")

    // Showing first 10 records
    ordDF.show(10)

    spark.stop()
  }
}
