import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_03 {

  def initializeSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  def readCsvToDataFrame(spark: SparkSession, filePath: String): DataFrame = {
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

  def castDateColumnToTimestamp(df: DataFrame): DataFrame = {
    df.withColumn("date", col("date").cast("TIMESTAMP"))
  }

  def identifyCommonDelays(df: DataFrame): DataFrame = {
  val winterMonthExpr = (month(col("date")) >= 12) || (month(col("date")) <= 2)
  val holidayExpr = dayofweek(col("date")).isin(1, 7)

  df.withColumn("Winter_Month", when(winterMonthExpr, lit("Yes")).otherwise(lit("No")))
    .withColumn("Holiday", when(holidayExpr, lit("Yes")).otherwise(lit("No")))
    .groupBy(date_format(col("date"), "MM-dd").alias("month_day"), col("Winter_Month"), col("Holiday"))
    .count()
    .orderBy(col("count").desc())
  }

  def labelDelayCategories(df: DataFrame): DataFrame = {
    val delayExpr = col("delay")
    df.withColumn("Flight_Delays",
      when(delayExpr > 360, "Very Long Delays")
        .when((delayExpr >= 120) && (delayExpr < 360), "Long Delays")
        .when((delayExpr >= 60) && (delayExpr < 120), "Short Delays")
        .when((delayExpr > 0) && (delayExpr < 60), "Tolerable Delays")
        .when(delayExpr === 0, "No Delays")
        .otherwise("Early"))
  }

  def createTemporaryTable(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView(tableName)
  }

  def extractMonthAndDay(df: DataFrame): DataFrame = {
    df.withColumn("month", month(col("date"))).withColumn("day", dayofmonth(col("date")))
  }

  def filterDataFrame(df: DataFrame): DataFrame = {
    df.filter(
      (col("origin") === "ORD") &&
        (month(col("date")) === 3) &&
        (dayofmonth(col("date")).between(1, 15))
    )
  }

  def showDataFrame(df: DataFrame, numRecords: Int = 5): Unit = {
    df.show(numRecords)
  }

  def listTableColumns(spark: SparkSession, tableName: String): Unit = {
    val tableColumns = spark.catalog.listColumns(tableName)
    println(s"Columns of $tableName:")
    tableColumns.foreach(column => println(column.name))
  }

  def writeDataFrameToJson(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").json(outputPath)
  }

  def writeDataFrameToLz4Json(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").format("json").option("compression", "lz4").save(outputPath)
  }

  def writeDataFrameToParquet(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").parquet(outputPath)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03 <file_path>")
      sys.exit(-1)
    }

    val spark = initializeSparkSession("assignment_03")
    val inputFilePath = args(0)

    var df = readCsvToDataFrame(spark, inputFilePath)
    val schema = defineDataFrameSchema()
    df = castDateColumnToTimestamp(df)
    val df1 = identifyCommonDelays(df)
    df1.show(10)

    val df2 = labelDelayCategories(df)
    df2.select("delay", "origin", "destination", "Flight_Delays").show(10)

    createTemporaryTable(df, "us_delay_flights_tbl")

    df = castDateColumnToTimestamp(df)
    df = extractMonthAndDay(df)

    val filteredDf = filterDataFrame(df)
    showDataFrame(filteredDf)

    listTableColumns(spark, "us_delay_flights_tbl")

    val jsonOutputPath = "departuredelays.json"
    writeDataFrameToJson(df, jsonOutputPath)

    val lz4JsonOutputPath = "departuredelays_lz4.json"
    writeDataFrameToLz4Json(df, lz4JsonOutputPath)

    val parquetOutputPath = "departuredelays.parquet"
    writeDataFrameToParquet(df, parquetOutputPath)

    println("Data has been written to the specified files.")

    val parquetFilePath = "departuredelays.parquet"
    df = spark.read.parquet(parquetFilePath)
    df = df.withColumn("date", to_date(col("date"), "MMddHHmm"))

    val ordDf = df.filter(col("origin") === "ORD")

    val ordParquetOutputPath = "orddeparturedelays.parquet"
    ordDf.write.mode("overwrite").parquet(ordParquetOutputPath)

    ordDf.show(10)
  }
}
