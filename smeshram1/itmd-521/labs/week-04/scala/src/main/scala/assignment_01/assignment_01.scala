package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment_01 {
  def main(args: Array[String]): Unit = {
    // Step 1: Create SparkSession
    val spark = SparkSession.builder().appName("DivvyTripsScala").getOrCreate()

    // Step 2: Read the Divvy_Trips_2015-Q1.csv file and infer the schema

    val Divvy_Trips_2015 = args(0)

    val dfInferred = spark.read.option("header", "true").csv(Divvy_Trips_2015)
    
    // Print the inferred schema
    println("Inferred Schema:")
    println(dfInferred.printSchema)
 
    // Step 3: Programmatically use StructFields to create and attach a schema
    val schema = StructType(Array(StructField("trip_id", IntegerType, true),
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
    
      val dfProgrammatic = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).csv(Divvy_Trips_2015)
    
      print("\nProgrammatically Attached Schema:")
      println(dfProgrammatic.printSchema)

    // Step 4: Attach a schema via DDL and read the CSV file
    val ddlSchema =
    "trip_id INTEGER, starttime STRING, stoptime STRING, bikeid INTEGER, tripduration INTEGER, from_station_id INTEGER, from_station_name STRING, to_station_id INTEGER, to_station_name STRING, usertype STRING, gender STRING, birthyear INTEGER"

    val dfDDL = spark.read.option("header", "true").option("inferSchema", "false").schema(ddlSchema).csv(Divvy_Trips_2015)

    print("\nDDL Attached Schema:")
    dfDDL.printSchema

    // Step 5: Display the number of records in each DataFrame
    println("Number of Records in Each DataFrame:")
    println("Inferred Schema DataFrame Count: " + dfInferred.count())
    println("Programmatically Attached Schema DataFrame Count: " + dfProgrammatic.count())
    println("DDL Attached Schema DataFrame Count: " + dfDDL.count())

    //Transformation and Actions

    dfInferred
     .select("gender", "to_station_name")
     .filter(col("gender") === "Female") // Filter rows where gender is "female"
     .groupBy("gender", "to_station_name")
     .count()
     .orderBy(desc("count"))
     .show(10, false) // Display 10 records of the DataFrame

    
    // Step 7: Stop SparkSession
    spark.stop()

  }
}