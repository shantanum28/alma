package main.scala.assignment_01
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object assignment_01 {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder
      .appName("Divvy Trips Analysis")
      .getOrCreate()

    // Define schema
    val customSchema = new StructType()
      .add("trip_id", IntegerType, nullable = true)
      .add("starttime", StringType, nullable = true)
      .add("stoptime", StringType, nullable = true)
      .add("bikeid", IntegerType, nullable = true)
      .add("tripduration", IntegerType, nullable = true)
      .add("from_station_id", IntegerType, nullable = true)
      .add("from_station_name", StringType, nullable = true)
      .add("to_station_id", IntegerType, nullable = true)
      .add("to_station_name", StringType, nullable = true)
      .add("usertype", StringType, nullable = true)
      .add("gender", StringType, nullable = true)
      .add("birthyear", StringType, nullable = true)

    // Read CSV with inferred schema
    val dfInferredSchema = spark.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")
    println("Inferred Schema:")
    dfInferredSchema.printSchema()
    println("Number of records: " + dfInferredSchema.count())

    // Read CSV with programmatically defined schema
    val dfProgrammaticSchema = spark.read.option("header", "true").schema(customSchema).csv("Divvy_Trips_2015-Q1.csv")
    println("\nProgrammatic Schema:")
    dfProgrammaticSchema.printSchema()
    println("Number of records: " + dfProgrammaticSchema.count())

    // Read CSV with schema via DDL
    val dfDDLSchema = spark.read.option("header", "true").csv("Divvy_Trips_2015-Q1.csv")
    dfDDLSchema.createOrReplaceTempView("divvy_trips")
    val dfDDLWithSchema = spark.sql("SELECT * FROM divvy_trips")
    println("\nSchema via DDL:")
    dfDDLWithSchema.printSchema()
    println("Number of records: " + dfDDLWithSchema.count())

    // Selecting Gender and filtering
    val dfFiltered = dfDDLSchema.select("gender").filter(dfDDLSchema("gender").equalTo("Female") || dfDDLSchema("gender").equalTo("Male"))
    println("\nFiltered DataFrame:")
    dfFiltered.show(10)

    // Select gender based on last name and group by station
    val selectedGender = dfDDLWithSchema.selectExpr("CASE WHEN substring(gender, 1, 1) >= 'A' AND substring(gender, 1, 1) <= 'K' THEN 'Female' ELSE 'Male' END AS selected_gender", "to_station_name").groupBy("to_station_name").count()
    println("\nSelected Gender:")
    selectedGender.show(10)

    // Stop the SparkSession
    spark.stop()
  }
}