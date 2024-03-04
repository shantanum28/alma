import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_03 {

  def initializeSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  val readCsvToDataFrame: (SparkSession, String) => DataFrame = (spark: SparkSession, file_path: String) => {
    spark.read.csv(file_path).toDF(defineDataFrameSchema().fieldNames: _*)
  }

  val defineDataFrameSchema: () => StructType = () => {
    StructType(Array(
      StructField("date", StringType, nullable = true),
      StructField("delay", IntegerType, nullable = true),
      StructField("distance", IntegerType, nullable = true),
      StructField("origin", StringType, nullable = true),
      StructField("destination", StringType, nullable = true)
    ))
  }

  val castDateColumnToTimestamp: DataFrame => DataFrame = (df: DataFrame) => {
    df.withColumn("date", col("date").cast("TIMESTAMP"))
  }

  val identifyCommonDelays: DataFrame => DataFrame = (df: DataFrame) => {
    val winterMonthExpr = (month(col("date")) >= 12) || (month(col("date")) <= 2)
    val holidayExpr = dayofweek(col("date")).isin(1, 7)

    df.withColumn("Winter_Month", when(winterMonthExpr, "Yes").otherwise("No"))
      .withColumn("Holiday", when(holidayExpr, "Yes").otherwise("No"))
      .groupBy(date_format(col("date"), "MM-dd").alias("month_day"), col("Winter_Month"), col("Holiday"))
      .count()
      .orderBy(col("count").desc())
  }

  val labelDelayCategories: DataFrame => DataFrame = (df: DataFrame) => {
    val delayExpr = col("delay")
    df.withColumn("Flight_Delays",
      when(delayExpr > 360, "Very Long Delays")
        .when((delayExpr >= 120) && (delayExpr < 360), "Long Delays")
        .when((delayExpr >= 60) && (delayExpr < 120), "Short Delays")
        .when((delayExpr > 0) && (delayExpr < 60), "Tolerable Delays")
        .when(delayExpr === 0, "No Delays")
        .otherwise("Early"))
  }

  val createTemporaryTable: DataFrame => Unit = (df: DataFrame) => {
    df.createOrReplaceTempView("us_delay_flights_tbl")
  }

  val extractMonthAndDay: DataFrame => DataFrame = (df: DataFrame) => {
    df.withColumn("month", month(col("date"))).withColumn("day", dayofmonth(col("date")))
  }

  val filterDataFrame: DataFrame => DataFrame = (df: DataFrame) => {
    df.filter(
      (col("origin") === "ORD") &&
        (month(col("date")) === 3) &&
        (dayofmonth(col("date")).between(1, 15))
    )
  }

  val showDataFrame: (DataFrame, Int) => Unit = (df: DataFrame, numRecords: Int) => {
    df.show(numRecords)
  }

  val listTableColumns: String => Unit = (tableName: String) => {
    val tableColumns = spark.catalog.listColumns(tableName)
    println(s"Columns of $tableName:")
    tableColumns.foreach(column => println(column.name))
  }

  val writeDataFrameToJson: (DataFrame, String) => Unit = (df: DataFrame, outputPath: String) => {
    df.write.mode("overwrite").json(outputPath)
  }

  val writeDataFrameToLz4Json: (DataFrame, String) => Unit = (df: DataFrame, outputPath: String) => {
    df.write.mode("overwrite").format("json").option("compression", "lz4").save(outputPath)
  }

  val writeDataFrameToParquet: (DataFrame, String) => Unit = (df: DataFrame, outputPath: String) => {
    df.write.mode("overwrite").parquet(outputPath)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit assignment_03 <file_path>")
      sys.exit(-1)
    }

    val inputFilePath: String = args(0)
    val df: DataFrame = readCsvToDataFrame(spark, inputFilePath)
    val dfWithTimestamp: DataFrame = castDateColumnToTimestamp(df)
    val df1: DataFrame = identifyCommonDelays(dfWithTimestamp)
    df1.show(10)

    val df2: DataFrame = labelDelayCategories(df)
    df2.select("delay", "origin", "destination", "Flight_Delays").show(10)

    createTemporaryTable(df)

    val dfWithTimestampAndMonthDay: DataFrame = extractMonthAndDay(df)
    val filteredDF: DataFrame = filterDataFrame(dfWithTimestampAndMonthDay)
    showDataFrame(filteredDF, 5)

    listTableColumns("us_delay_flights_tbl")

    val jsonOutputPath: String = "departuredelays.json"
    writeDataFrameToJson(df, jsonOutputPath)

    val lz4JsonOutputPath: String = "departuredelays_lz4.json"
    writeDataFrameToLz4Json(df, lz4JsonOutputPath)

    val parquetOutputPath: String = "departuredelays.parquet"
    writeDataFrameToParquet(df, parquetOutputPath)

    println("Data has been written to the specified files.")

    val parquetFilePath: String = "departuredelays.parquet"
    val dfFromParquet: DataFrame = spark.read.parquet(parquetFilePath)
    val dfWithDate: DataFrame = dfFromParquet.withColumn("date", to_date(col("date"), "MMddHHmm"))

    val ordDF: DataFrame = dfWithDate.filter(col("origin") === "ORD")

    val ordParquetOutputPath: String = "orddeparturedelays.parquet"
    ordDF.write.mode("overwrite").parquet(ordParquetOutputPath)

    ordDF.show(10)
  }
}
