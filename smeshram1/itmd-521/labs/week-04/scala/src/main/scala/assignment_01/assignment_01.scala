package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DivvyTripsScala").getOrCreate()

    val Divvy_Trips_2015 = args(0)

    
    val dfInferred = spark.read.option("header", "true").csv(Divvy_Trips_2015)

    
    println("Inferred Schema:")
    println(dfInferred.printSchema)

    
    dfInferred
      .select("gender", "to_station_name")
      .filter(col("gender") === "Female") // Filter rows where gender is "female"
      .groupBy("gender", "to_station_name")
      .count()
      .orderBy(desc("count"))
      .show(10, false) // Display 10 records of the DataFrame

    
    val schema = StructType(Array(
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

    val dfProgrammatic = spark.read.option("header", "true").option("inferSchema", "false").schema(schema).csv(Divvy_Trips_2015)

    print("\nProgrammatically Attached Schema:")
    println(dfProgrammatic.printSchema)

    
    val ddlSchema =
      "trip_id INTEGER, starttime STRING, stoptime STRING, bikeid INTEGER, tripduration INTEGER, from_station_id INTEGER, from_station_name STRING, to_station_id INTEGER, to_station_name STRING, usertype STRING, gender STRING, birthyear INTEGER"

    val dfDDL = spark.read.option("header", "true").option("inferSchema", "false").schema(ddlSchema).csv(Divvy_Trips_2015)

    print("\nDDL Attached Schema:")
    dfDDL.printSchema

    
    println("Number of Records in Each DataFrame:")
    println("Inferred Schema DataFrame Count: " + dfInferred.count())
    println("Programmatically Attached Schema DataFrame Count: " + dfProgrammatic.count())
    println("DDL Attached Schema DataFrame Count: " + dfDDL.count())

    
    spark.stop()
  }
}