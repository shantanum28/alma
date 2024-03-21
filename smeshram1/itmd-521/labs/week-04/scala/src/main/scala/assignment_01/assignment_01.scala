package main.scala.assignment_01

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_01 {
  def main(args: Array[String]){
    // Initializing a SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("assignment_01")
      .getOrCreate()

    // Reading CSV data with inferred schema
    val dfInferred: DataFrame = spark.read.option("header", true).csv("Divvy_Trips_2015-Q1.csv")
    println("DataFrame with inferred schema:")
    dfInferred.printSchema()
    println("Number of records:", dfInferred.count())

    // Creating schema programmatically and reading CSV
    val schemaFields: Array[StructField] = Array(
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
      StructField("birthyear", StringType, nullable = true)
    )
    val programmaticSchema: StructType = StructType(schemaFields)
    val dfProgrammatic: DataFrame = spark.read.option("header", true).schema(programmaticSchema).csv("Divvy_Trips_2015-Q1.csv")
    println("\nDataFrame with programmatically created schema:")
    dfProgrammatic.printSchema()
    println("Number of records:", dfProgrammatic.count())

    // Attaching schema via DDL and reading CSV
    val ddlSchemaStr: String = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"
    val dfDDL: DataFrame = spark.read.option("header", true).schema(ddlSchemaStr).csv("Divvy_Trips_2015-Q1.csv")
    println("\nDataFrame with schema attached via DDL:")
    dfDDL.printSchema()
    println("Number of records:", dfDDL.count())

    // Selecting Gender and filtering
    val dfFiltered: DataFrame = dfDDL.select("gender").filter($"gender" === "Female" || $"gender" === "Male")
    println("\nFiltered DataFrame:")
    dfFiltered.show(10)

    // GroupingBy station to
    val dfGrouped: DataFrame = dfDDL.groupBy("to_station_name").count()
    println("\nGrouped DataFrame:")
    dfGrouped.show(10)

    // Stopping SparkSession
    spark.stop()
  }
}