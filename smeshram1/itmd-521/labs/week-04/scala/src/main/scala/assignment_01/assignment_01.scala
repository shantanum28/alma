package main.scala.assignment_01

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataFrame Transformations and Actions")
      .getOrCreate()

    val filePath = args(0)

    // First: Read the CSV file with inferred schema
    val dfInferredSchema = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
    println("DataFrame with Inferred Schema - Scala:")
    dfInferredSchema.printSchema()
    println("Number of records:", dfInferredSchema.count())

    // Second: Programmatically specify schema using StructFields
    val schemaProgrammatic = StructType(
      Array(
        StructField("trip_id", IntegerType, nullable = true),
        StructField("starttime", StringType, nullable = true),
        StructField("stoptime", StringType, nullable = true),
        StructField("bikeid", IntegerType, nullable = true),
        StructField("tripduration", DoubleType, nullable = true),
        StructField("from_station_id", IntegerType, nullable = true),
        StructField("from_station_name", StringType, nullable = true),
        StructField("to_station_id", IntegerType, nullable = true),
        StructField("to_station_name", StringType, nullable = true),
        StructField("usertype", StringType, nullable = true),
        StructField("gender", StringType, nullable = true),
        StructField("birthyear", IntegerType, nullable = true)
      )
    )

    val dfSpecifiedSchema = spark.read
      .schema(schemaProgrammatic)
      .option("header", "true")
      .csv(filePath)
    println("\nDataFrame with Programmatical Schema - Scala:")
    dfSpecifiedSchema.printSchema()
    println("Number of records:", dfSpecifiedSchema.count())

    // Third: Attach schema via DDL
    val schemaDDL = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration DOUBLE, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val dfDDLSchema = spark.read
      .schema(schemaDDL)
      .option("header", "true")
      .csv(filePath)
    println("\nDataFrame with Schema attached via DDL - Scala:")
    dfDDLSchema.printSchema()
    println("Number of records:", dfDDLSchema.count())

    // Select gender based on last name
    val dfWithGender = dfInferredSchema.withColumn("gender",
      when(substring(col("from_station_name"), 1, 1).between("A", "K"), lit("Female"))
        .otherwise(lit("Male"))
    )

    // Group by station name
    val groupedDF = dfWithGender.groupBy("from_station_name")
      .agg(count("*").as("count"))

    // Show 10 records
    println("\nDataFrame with Gender Selection and Grouping by Station Name:")
    groupedDF.show(10)

    spark.stop()
  }
}