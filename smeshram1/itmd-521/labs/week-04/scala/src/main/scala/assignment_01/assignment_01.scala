package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,TimestampType}

object  assignment_01 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("assignment_01")
      .getOrCreate()

    if (args.length < 1) {
      sys.exit(1)
    }

    val filePath = args(0)
    // First read: Infer schema
    val df1 = spark.read.option("header", "true").csv(filePath)
    println("Inferred Schema:")
    df1.printSchema()
    println("Number of records:", df1.count())

    // Second read: Programmatically define schema
    val schema = new StructType()
    .add("trip_id", IntegerType, nullable = true)
    .add("starttime", TimestampType, nullable = true)
    .add("stoptime", TimestampType, nullable = true)
    .add("bikeid", IntegerType, nullable = true)
    .add("tripduration", IntegerType, nullable = true)
    .add("from_station_id", IntegerType, nullable = true)
    .add("from_station_name", StringType, nullable = true)
    .add("to_station_id", IntegerType, nullable = true)
    .add("to_station_name", StringType, nullable = true)
    .add("usertype", StringType, nullable = true)
    .add("gender", StringType, nullable = true)
    .add("birthyear", IntegerType, nullable = true)

    val df2 = spark.read.schema(schema).option("header", "true").csv(filePath)
    println("Programmatically Defined Schema:")
    df2.printSchema()
    println("Number of records:", df2.count())

    // Third read: Schema via DDL
    val ddlSchema = "trip_id INT, starttime TIMESTAMP, stoptime TIMESTAMP, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val df3 = spark.read.schema(ddlSchema).option("header", "true").csv(filePath)
    println("Schema via DDL:")
    df3.printSchema()
    println("Number of records:", df3.count())


    val stationTo = df1
    .select("trip_id", "to_station_name", "gender")
    .where(col("gender") === "Female")
    .groupBy("to_station_name")
    .count()

    println("\nGrouped Data:")
    stationTo.show(10)

    println(df1.printSchema)
    println(df1.schema)

    spark.stop()
  }
}