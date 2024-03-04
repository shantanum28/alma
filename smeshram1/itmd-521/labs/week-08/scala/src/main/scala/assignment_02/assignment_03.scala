import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.functions._

object assignment_03 {
  def initializeSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  def readCsvToDataFrame(spark: SparkSession, filePath: String) = {
    spark.read.csv(filePath).toDF()
  }

  def defineDataFrameSchema() = {
    StructType(
      Array(
        StructField("date", StringType, true),
        StructField("delay", IntegerType, true),
        StructField("distance", IntegerType, true),
        StructField("origin", StringType, true),
        StructField("destination", StringType, true)
      )
    )
  }

  def castDateColumnToTimestamp(df: org.apache.spark.sql.DataFrame) = {
    df.withColumn("date", col("date").cast("TIMESTAMP"))
  }

  def identifyCommonDelays(df: org.apache.spark.sql.DataFrame) = {
    val winterMonthExpr = (month(col("date")) >= 12) || (month(col("date")) <= 2)
    val holidayExpr = dayofweek(col("date")).isin(1, 7)

    df.withColumn("Winter_Month", when(winterMonthExpr, "Yes").otherwise("No"))
      .withColumn("Holiday", when(holidayExpr, "Yes").otherwise("No"))
      .groupBy(date_format("date", "MM-dd").alias("month_day"), "Winter_Month", "Holiday")
      .count()
      .orderBy(col("count").desc())
  }

  def labelDelayCategories(df: org.apache.spark.sql.DataFrame) = {
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

  def createTemporaryTable(df: org.apache.spark.sql.DataFrame) = {
    df.createOrReplaceTempView("us_delay_flights_tbl")
  }

  def extractMonthAndDay(df: org.apache.spark.sql.DataFrame) = {
    df.withColumn("month", month("date")).withColumn("day", dayofmonth("date"))
  }

  def filterDataFrame(df: org.apache.spark.sql.DataFrame) = {
    df.filter(
      (col("origin") === "ORD") &&
        (month("date") === 3) &&
        (dayofmonth("date").between(1, 15))
    )
  }

  def showDataFrame(df: org.apache.spark.sql.DataFrame, numRecords: Int = 5) = {
    df.show(numRecords)
  }

  def listTableColumns(spark: SparkSession, tableName: String) = {
    val tableColumns = spark.catalog.listColumns(tableName)
    println(s"Columns of $tableName:")
    tableColumns.foreach(column => println(column.name))
  }

  def writeDataFrameToJson(df: org.apache.spark.sql.DataFrame, outputPath: String) = {
    df.write.mode("overwrite").json(outputPath)
  }

  def writeDataFrameToLz4Json(df: org.apache.spark.sql.DataFrame, outputPath: String) = {
    df.write.mode("overwrite").format("json").option("compression", "lz4").save(outputPath)
  }

  def writeDataFrameToParquet(df: org.apache.spark.sql.DataFrame, outputPath: String) = {
    df.write.mode("overwrite").parquet(outputPath)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit --class assignment_03 <jar-file> <file_path>")
      System.exit(1)
    }

    val spark = initializeSparkSession("assignment_03")
    val inputFilePath = args(0)

    val df = readCsvToDataFrame(spark, inputFilePath)
    val schema = defineDataFrameSchema()
    val castedDf = castDateColumnToTimestamp(df)
    val df1 = identifyCommonDelays(castedDf)
    df1.show(10)

    val df2 = labelDelayCategories(castedDf)
    df2.select("delay", "origin", "destination", "Flight_Delays").show(10)

    createTemporaryTable(castedDf)

    val dfWithTimestamp = castDateColumnToTimestamp(df)
    val dfWithMonthAndDay = extractMonthAndDay(dfWithTimestamp)

    val filteredDf = filterDataFrame(dfWithMonthAndDay)
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
    val parquetDf = spark.read.parquet(parquetFilePath)
    val dfWithDate = parquetDf.withColumn("date", to_date(col("date"), "MMddHHmm"))

    val ordDf = dfWithDate.filter(col("origin") === "ORD")

    val ordParquetOutputPath = "orddeparturedelays.parquet"
    ordDf.write.mode("overwrite").parquet(ordParquetOutputPath)

    ordDf.show(10)
  }
}
