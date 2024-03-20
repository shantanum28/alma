package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Scala Spark Schema Assignment")
      .getOrCreate()

    //Get file path from command line arguments
    val file_path = if (args.length > 0) args(0) else "default/path/to/csv/file.csv"

    // First read: Infer schema
    val df1 = spark.read.option("header", "true").csv(file_path)
    println("Inferred Schema:")
    df1.printSchema()
    println("Number of records:", df1.count())

    // Second read: Programmatically define schema
    val schema = StructType(Array(
      StructField("trip_id", IntegerType, true),
      StructField("starttime", StringType, false),
      StructField("stoptime", StringType, true),
      StructField("bikeid", IntegerType, true),
      StructField("tripduration", IntegerType, true),
      StructField("from_station_id", IntegerType, true),
      StructField("from_station_name", StringType, false),
      StructField("to_station_id", IntegerType, true),
      StructField("to_station_name", StringType, true),
      StructField("usertype", StringType, false),
      StructField("gender", StringType, true),
      StructField("birthyear", IntegerType, true)))
    val df2 = spark.read.schema(schema).option("header", "true").csv(file_path)
    println("Programmatically Defined Schema:")
    df2.printSchema()
    println("Number of records:", df2.count())

    // Third read: Schema via DDL
    val ddl_schema = "trip_id INTEGER,starttime STRING,stoptime STRING,bikeid INTEGER,tripduration INTEGER,from_station_id INTEGER,from_station_name STRING,to_station_id INTEGER,to_station_name STRING,usertype STRING,gender STRING,birthyear INTEGER"
    val df3 = spark.read.schema(ddl_schema).option("header", "true").csv(file_path)
    println("Schema via DDL:")
    df3.printSchema()
    println("Number of records:", df3.count())

    // DataFrame operations
    val gender_df = df1.select("*")
      .where(df1("gender") === "Female")
      .groupBy("to_station_name")
      .count()
    gender_df.show(10, truncate = false)


    // Stop SparkSession
    spark.stop()
  }
}