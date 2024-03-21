package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_01 {
    def main(args: Array[String]){
      if (args.length < 1){
        println("Usage: DivvyTripsAnalysis <path to file>")
        sys.exit(-1)
      }
    
    // Creating SparkSession
    val spark = SparkSession
        .builder
        .appName("Divvy Trips Analysis")
        .getOrCreate()

    // Get Divvy Trips data set filename
    val divvy_data_file = args(0)

    // Inferring printSchema and printing the count
    val infer_trips_df = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(divvy_data_file)

    println("Scala inferred printSchema for Divvy Trips")
    infer_trips_df.printSchema()
    println(s"Total number of Divvy trips: ${infer_trips_df.count()}")

    // Reading datafile programmatically and printing the count
    println("Scala Programmatic printSchema for Divvy Trips")
    val struct_divvy_schema = StructType(Array(
      StructField("trip_id", IntegerType, true),
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
      StructField("birthyear", IntegerType, true)))

    val prog_trips_df = spark.read.format("csv")
      .option("header", "true")
      .schema(struct_divvy_schema)
      .load(divvy_data_file)

    prog_trips_df.printSchema()
    println(s"Total number of Divvy trips: ${prog_trips_df.count()}")

    // Reading datafile using DDL and printing the count
    println("Scala DDL printSchema for Divvy Trips")
    val ddl_divvy_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    val ddl_trips_df = spark.read.format("csv")
      .option("header", "true")
      .schema(ddl_divvy_schema)
      .load(divvy_data_file)

    ddl_trips_df.printSchema()
    println(s"Total number of Divvy trips: ${ddl_trips_df.count()}")

    // Displaying 10 results for female grouping by to_station_name
    val fem_trips_df = infer_trips_df.select("to_station_name","gender")
      .where(col("gender") === "Female")
      .groupBy("to_station_name")
      .count()

    println("Scala: Displaying 10 results for female grouping by to_station_name")
    fem_trips_df.show(10, false)

    // Stop SparkSession
    spark.stop()
  }
}