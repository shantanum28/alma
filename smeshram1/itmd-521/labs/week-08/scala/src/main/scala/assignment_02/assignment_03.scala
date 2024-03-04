import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_03 {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03.scala <input_file_path>")
      System.exit(1)
    }

    val inputFilePath = args(0)

    val spark = SparkSession.builder
      .appName("assignment_03")
      .getOrCreate()

    try {
      // Part I - Query 1
      val departuresDF = spark.read.option("header", "true").csv(inputFilePath)
      departuresDF.createOrReplaceTempView("departures")

      val query1Result = departuresDF
        .filter("delay > 0")
        .groupBy("origin", "destination")
        .agg(count("*").alias("total_delays"))
        .orderBy(desc("total_delays"))
        .limit(10)

      query1Result.show()

      // Part I - Query 2
      val query2Result = departuresDF
        .withColumn("Flight_Delays", when(col("delay") > 360, "Very Long Delays")
          .when(col("delay") > 120 && col("delay") <= 360, "Long Delays")
          .otherwise("Other Delays"))
        .select("origin", "destination", "delay", "Flight_Delays")
        .limit(10)

      query2Result.show()

      // Part II - Creating table and tempView
      departuresDF.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

      val chicagoFlights = departuresDF
        .filter("origin = 'ORD' AND month = 3 AND day >= 1 AND day <= 15")
        .limit(5)
      chicagoFlights.createOrReplaceTempView("chicago_flights_view")
      chicagoFlights.show()

      // Part II - Using Spark Catalog
      spark.conf.set("spark.sql.catalogImplementation", "hive")
      val catalog = spark.catalog
      val columns = catalog.listColumns("us_delay_flights_tbl")
      columns.show()

      // Part III - Reading and writing DataFrame
      val departuresSchema = StructType(Seq(
        StructField("date", DateType),
        StructField("delay", IntegerType),
        // Add other schema fields here
      ))

      val departuresWithSchema = spark.read
        .schema(departuresSchema)
        .option("header", "true")
        .csv(inputFilePath)

      departuresWithSchema.write.mode("overwrite").json("departuredelays.json")
      departuresWithSchema.write.mode("overwrite").option("compression", "lz4").json("departuredelays_lz4.json")
      departuresWithSchema.write.mode("overwrite").parquet("departuredelays.parquet")

      // Part IV - Reading Parquet file and filtering ORD records
      val parquetDF = spark.read.parquet("departuredelays.parquet")
      val ordDepartures = parquetDF.filter("origin = 'ORD'")
      ordDepartures.write.mode("overwrite").parquet("orddeparturedelays.parquet")
      ordDepartures.show(10)
    } finally {
      spark.stop()
    }
  }
}
