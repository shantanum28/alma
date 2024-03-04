import org.apache.spark.sql.{SparkSession, SaveMode}

object assignment_03 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03.scala <path-to-departuredelays.csv>")
      System.exit(1)
    }

    val filePath = args(0)

    val spark = SparkSession.builder()
      .appName("assignment_03")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    // Part I
    val departuresDF = spark.read.format("csv").option("header", true).load(filePath)
    departuresDF.createOrReplaceTempView("departures")

    // Spark SQL Query 1
    val query1Result = spark.sql("SELECT DISTINCT destination FROM departures")
    query1Result.show(10)

    // Spark SQL Query 2
    val query2Result = spark.sql("SELECT COUNT(*) as count, destination FROM departures GROUP BY destination")
    query2Result.show(10)

    // Part II
    departuresDF.createOrReplaceTempView("us_delay_flights_tbl")
    val usFlightsView = spark.sql("SELECT * FROM us_delay_flights_tbl WHERE origin = 'ORD' AND SUBSTRING(date, 1, 5) BETWEEN '03/01' AND '03/15'")
    usFlightsView.show(5)

    // Spark Catalog
    spark.catalog.listColumns("us_delay_flights_tbl").show()

    // Part III
    val departuresWithSchema = departuresDF.withColumn("date", departuresDF("date").cast("date"))
    departuresWithSchema.write.mode(SaveMode.Overwrite).json("departuredelays.json")
    departuresWithSchema.write.mode(SaveMode.Overwrite).option("compression", "lz4").json("departuredelays_lz4.json")
    departuresWithSchema.write.mode(SaveMode.Overwrite).parquet("departuredelays.parquet")

    // Part IV
    val departuresParquet = spark.read.parquet("departuredelays.parquet")
    val ordDepartures = departuresParquet.filter("origin = 'ORD'")
    ordDepartures.write.mode(SaveMode.Overwrite).parquet("orddeparturedelays.parquet")
    ordDepartures.show(10)
    
    spark.stop()
  }
}
