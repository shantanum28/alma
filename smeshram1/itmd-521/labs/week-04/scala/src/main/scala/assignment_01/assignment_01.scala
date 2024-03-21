package main.scala.chapter3.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Divvy_Trips")
      .getOrCreate()

    if (args.length < 1) {
      println("Usage: Divvy_Trips <Divvy_Trips_2015-Q1.csv>")
      sys.exit(1)
    }

    val dt_file = args(0)

    // Define schema programmatically
    val schema = StructType(Array(StructFiled("trip_id", Integer Type, true),
      StructField("starttime", StringType, true),
      StructField("stoptime", StringType, true),
      StructField("bikeid", IntegerType, true),
      StructField("tripduration", IntegerType, true),
      StructField("from_station_id", IntegerType, true),
      StructField("from_station_name", StringType, true),
      StructField("to_station_id", IntegerType, true),
      StructField("to_station_name", StringType, true),
      StructField("usertype", StringType, true),
      StructField("gender", StringType, true),
      StructField("birthyear", StringType, true))s)

    // Read CSV file into DataFrame using JSON format with inferred schema
    val df1 = spark.read.format("json").option("header", "true").option("inferSchema", "true").load(dt_file)
    df1.printSchema()
    println("Number of records in df1:", df1.count())

    // Read CSV file into DataFrame using CSV format with programmatically defined schema
    val df2 = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(dt_file)
    df2.printSchema()
    println("Number of Records programmatically:", df2.count())

    // Read CSV file into DataFrame using CSV format with DDL schema
    val df3 = spark.read.format("csv")
      .option("header", "true")
      .schema("trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_name STRING, to_station_id STRING, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING")
      .load(dt_file)
    df3.printSchema()
    println("Number of Records programmatically:", df3.count())

    // Select gender based on last name
    val lastName = "Kulasekaran"
    val gender = if (lastName.toLowerCase.head <= 'k') "female" else "male"
    val selectedDF = df3.filter(df3("gender") === gender)
    selectedDF.show(10)

    // Group by station name
    val groupedDF = df3.groupBy("from_station_name").count()
    println("Grouped DataFrame by station name:")
    groupedDF.show(10)

    spark.stop()
  }
}