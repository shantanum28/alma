package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_01 {
 def main(args: Array[String]) {
   val spark = SparkSession
     .builder
     .appName("DivvyTrips")
     .getOrCreate()
   if (args.length < 1) {
     print("Usage: assignment_01 <file>")
     sys.exit(1)
   }

// Defining file path
    val divvyFile = args(0)

// Creating DataFrame schema
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
        StructField("birthyear", IntegerType, nullable = true)
      )
    )

// Read the CSV file using infer and print schema and count for each DataFrame
    val divvyDfInfer = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(divvyFile)
    divvyDfInfer.printSchema()
    println(s"Number of records in scala- infer schema: ${divvyDfInfer.count()}")

// Read the CSV file using structfield and print schema and count for each DataFrame
    val divvyDfStructFields = spark.read.format("csv")
      .option("header", "true")
      .schema(divvySchema)
      .load(divvyFile)
    divvyDfStructFields.printSchema()
    println(s"Number of records in scala- StructFields schema: ${divvyDfStructFields.count()}")

// DDL Schema
    val divvyDdlSchema ="trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING,to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT".stripMargin
    val divvyDfDdl = spark.read.format("csv")
      .option("header", "true")
      .option("schema", divvyDdlSchema)
      .load(divvyFile)
    divvyDfDdl.printSchema()
     println(s"Number of records in scala- DDL schema: ${divvyDfDdl.count()}")

// Select statement for gender, to_station_name grouping and counting
    val divvySelectedDf = divvyDfDdl.select("to_station_name", "gender")
      .where(col("gender") === "Male")
      .groupBy("to_station_name")
      .count()

// Show 10 records of the DataFrame
    divvySelectedDf.show(10, truncate = false)

// Stop Spark
    spark.stop()
  }
}