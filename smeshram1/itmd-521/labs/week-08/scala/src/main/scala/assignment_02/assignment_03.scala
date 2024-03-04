import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_03 {

  def initializeSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  def readCsvToDataFrame(spark: SparkSession, filePath: String): org.apache.spark.sql.DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
  }

  def defineDataFrameSchema(): StructType = {
    StructType(Seq(
      StructField("date", StringType, nullable = true),
      StructField("delay", IntegerType, nullable = true),
      StructField("distance", IntegerType, nullable = true),
      StructField("origin", StringType, nullable = true),
      StructField("destination", StringType, nullable = true)
    ))
  }

  def castDateColumnToTimestamp(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    df.withColumn("date", col("date").cast("TIMESTAMP"))
  }

  def labelDelayCategories(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val delayExpr = col("delay")
    df.withColumn("Flight_Delays",
      when(delayExpr > 360, "Very Long Delays")
        .when((delayExpr >= 120) && (delayExpr < 360), "Long Delays")
        .when((delayExpr >= 60) && (delayExpr < 120), "Short Delays")
        .when((delayExpr > 0) && (delayExpr < 60), "Tolerable Delays")
        .when(delayExpr === 0, "No Delays")
        .otherwise("Early")
    )
  }

  def createTemporaryTable(df: org.apache.spark.sql.DataFrame): Unit = {
    df.createOrReplaceTempView("us_delay_flights_tbl")
  }

  def extractMonthAndDay(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    df.withColumn("month", month("date")).withColumn("day", dayofmonth("date"))
  }


  def listTableColumns(spark: SparkSession, tableName: String): Unit = {
    val tableColumns = spark.catalog.listColumns(tableName)
    println(s"Columns of $tableName:")
    tableColumns.foreach(column => println(column.name))
  }

  def writeDataFrameToJson(df: org.apache.spark.sql.DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").json(outputPath)
  }

  def writeDataFrameToLz4Json(df: org.apache.spark.sql.DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").option("compression", "lz4").json(outputPath)
  }

  def writeDataFrameToParquet(df: org.apache.spark.sql.DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").parquet(outputPath)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03.jar <file_path>")
      sys.exit(-1)
    }

    val spark = initializeSparkSession("assignment_03")
    val inputFilePath = args(0)

    var df = readCsvToDataFrame(spark, inputFilePath)
    df = castDateColumnToTimestamp(df)

    // Part II
    createTemporaryTable(df)
    val filteredDF = filterDataFrame(df)
    filteredDF.show(5)
    listTableColumns(spark, "us_delay_flights_tbl")

    // Part III
    val jsonOutputPath = "departuredelays.json"
    writeDataFrameToJson(df, jsonOutputPath)

    val lz4JsonOutputPath = "departuredelays_lz4.json"
    writeDataFrameToLz4Json(df, lz4JsonOutputPath)

    val parquetOutputPath = "departuredelays.parquet"
    writeDataFrameToParquet(df, parquetOutputPath)

    // Part IV
    val parquetFilePath = "departuredelays.parquet"
    df = spark.read.parquet(parquetFilePath)
    df = df.withColumn("date", to_date(col("date"), "MMddHHmm"))

    val ordDF = df.filter(col("origin") === "ORD")
    val ordParquetOutputPath = "orddeparturedelays.parquet"
    ordDF.write.mode("overwrite").parquet(ordParquetOutputPath)
    ordDF.show(10)
  }
}
