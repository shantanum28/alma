package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_01 {
def main(args: Array[String]) {
 val spark = SparkSession
     .builder
     .appName("Divvytrip")
     .getOrCreate()

 if (args.length < 1) {
     print("Usage: assignment_01 <file>")
     sys.exit(1)
   }
// Get the Divvy Trips CSV file path from command-line argument
    val divvyFile = args(0)

// Define schema for DataFrame creation
    val divvySchema = StructType(
      Array(
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
        StructField("birthyear", IntegerType, nullable = true)))

// Read CSV file with schema inference
    val dfInfer = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(divvyFile)
    dfInfer.printSchema()
    println(s"Number of records: ${dfInfer.count()}")
// Read CSV file with specified schema
    val dfStructFields = spark.read.format("csv")
      .option("header", "true")
      .schema(divvySchema)
      .load(divvyFile)
    dfStructFields.printSchema()
    println(s"Number of records: ${dfStructFields.count()}")
// Read CSV file with schema defined via DDL
    val ddlSchema ="trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration DOUBLE,from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING,usertype STRING, gender STRING, birthyear INT"
 val dfDDL = spark.read.format("csv")
      .option("header", "true")
      .schema(divvySchema)
      .load(divvyFile)
    dfDDL.printSchema()
    println(s"Number of records: ${dfDDL.count()}")
// Select gender and to_station_name
    val selectedDF = dfDDL.select("gender", "to_station_name")
      .filter(col("gender") === "Male")
      .groupBy("to_station_name")
      .count()
    
    // Show 10 records of the DataFrame
    selectedDF.show(10,truncate=false)

    // Stop SparkSession
    spark.stop()
  }
}