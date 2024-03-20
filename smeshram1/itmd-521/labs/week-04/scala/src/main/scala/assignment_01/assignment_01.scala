package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when, count}

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Assignment 01 - Scala")
      .getOrCreate()

    // Read CSV file with schema inference
    val df = spark.read
      .option("header", "true")
      .csv("data/Divvy_Trips_2015-Q1.csv")

    // Print Schema
    println("Schema inferred:")
    df.printSchema()

    // Display number of records
    println("Number of records: " + df.count())

    // define schema programmatically using StructFields
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    val schema = StructType(Array(
      StructField("trip_id", IntegerType, nullable = true),
      StructField("starttime", StringType, nullable = true),
      StructField("stoptime", StringType, nullable = true),
      StructField("bikeid", IntegerType, nullable = true),
      StructField("tripduration", IntegerType, nullable = true),
      StructField("from_station_id", IntegerType, nullable = true),
      StructField("from_station_name", StringType, nullable = true),
      StructField("to_station_id", IntegerType, nullable = true),
      StructField("to_station_name", StringType, nullable = true),
      StructField("usertype", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("birthyear", IntegerType, nullable = true)
    ))

    // Read CSV file with programmatically defined schema
    val dfProgrammaticSchema = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("data/Divvy_Trips_2015-Q1.csv")

    // Print Schema
    println("Programmatically defined schema:")
    dfProgrammaticSchema.printSchema()

    // Display number of records
    println("Number of records: " + dfProgrammaticSchema.count())

    // Attach schema via DDL and read the CSV file
    val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, " +
      "from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, " +
      "usertype STRING, gender STRING, birthyear INT"
    val dfDdlSchema = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(ddlSchema)
      .csv("data/Divvy_Trips_2015-Q1.csv")

    // Print Schema
    println("Schema attached via DDL:")
    dfDdlSchema.printSchema()

    // Display number of records
    println("Number of records: " + dfDdlSchema.count())

     // Select Gender based on the last name first letter
    val dfWithGender = dfProgrammaticSchema.withColumn("Gender", when(col("to_station_name").rlike("[A-K]"), "Female").otherwise("Male"))

    // GroupBy the field "to_station_name"
    val dfGrouped = dfWithGender.groupBy("to_station_name").agg(count("to_station_name").alias("Number_of_Trips"))

    // Show 10 records of the resulting DataFrame
    println("Grouped by to_station_name:")
    dfGrouped.show(10)

    // Stop SparkSession
    spark.stop()
  }
}