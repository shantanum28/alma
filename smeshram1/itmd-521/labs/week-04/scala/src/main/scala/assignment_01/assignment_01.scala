package main.scala.assignment_01
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment_01 {
  def main(args: Array[String]) {
    // Initializing SparkSession
    val spark = SparkSession.builder
      .appName("Unique Divvy Trips Analysis")
      .getOrCreate()

    if (args.length < 1){
      println("usage: UniqueDivvyTripsData <file>")
      sys.exit(-1)

    }
    // Reading CSV data with inferred schema

    val divvydata = args(0)
    println(s" Inferred Schema :")
    val dfInferredSchema = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(divvydata)
    dfInferredSchema.printSchema()
    println(s"Number of records : ${dfInferredSchema.count()}")

    // Creating schema programmatically and reading CSV
    val programmaticSchema = StructType(Array(
      StructField("trip_id", IntegerType,  true),
      StructField("starttime", StringType,  true),
      StructField("stoptime", StringType,  true),
      StructField("bikeid", IntegerType,  true),
      StructField("tripduration", IntegerType,  true),
      StructField("from_station_id", IntegerType,  true),
      StructField("from_station_name", StringType,  true),
      StructField("to_station_id", IntegerType,  true),
      StructField("to_station_name", StringType,  true),
      StructField("usertype", StringType,  true),
      StructField("gender", StringType,  true),
      StructField("birthyear", StringType,  true)
    ))


    val dfProgrammaticSchema = spark.read.format("csv")
        .option("header", "true")
        .schema(programmaticSchema)
        .load(divvydata)
    dfProgrammaticSchema.printSchema()
    println(s"Number of records : ${dfProgrammaticSchema.count()}")


    // Attaching schema via DDL and reading CSV

    val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"

    val dfDDLSchema = spark.read.format("csv")
        .option("header", "true")
        .schema(ddlSchema)
        .load(divvydata)
    dfDDLSchema.printSchema()
    println(s"Number of records : ${dfDDLSchema.count()}")



    // Selecting Gender and filtering
    val dfgender = dfDDLSchema.select("*")
        .where(col("gender") === "Male")
        .groupBy("to_station_name")
        .count()

    dfgender.show(10,truncate = false)

    // Stopping SparkSession
    spark.stop()
  }
}