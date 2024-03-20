package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,TimestampType}

object  assignment_01 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("assignment_01")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    if (args.length < 1) {
      sys.exit(1)
    }

    val filePath = args(0)
    // First read: Infer schema
    val infer_df = spark.read.option("header", "true").csv(filePath)
    println("Inferred Schema:")
    infer_df.printSchema()
    println("Number of records:", infer_df.count())

    // Second read: Programmatically define schema
    val programmatically_schema = new StructType()
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
    .add("birthyear", IntegerType, nullable = true)

    val programmatically_df = spark.read.schema(programmatically_schema).option("header", "true").csv(filePath)
    // Convert starttime and stoptime columns to TimestampType
    val programmatically_df_timestamp = programmatically_df
    .withColumn("starttime", to_timestamp(programmatically_df("starttime"), "MM/dd/yyyy HH:mm"))
    .withColumn("stoptime", to_timestamp(programmatically_df("stoptime"), "MM/dd/yyyy HH:mm"))

    println("Programmatically Defined Schema:")
    programmatically_df_timestamp.printSchema()
    println("Number of records:", programmatically_df_timestamp.count())

    // Third read: Schema via DDL
    val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val ddl_df = spark.read.schema(ddlSchema).option("header", "true").csv(filePath)
    // Convert starttime and stoptime columns to TimestampType
    val ddl_df_timestamp = ddl_df
    .withColumn("starttime", to_timestamp(ddl_df("starttime"), "MM/dd/yyyy HH:mm"))
    .withColumn("stoptime", to_timestamp(ddl_df("stoptime"), "MM/dd/yyyy HH:mm"))

    println("Schema via DDL:")
    ddl_df_timestamp.printSchema()
    println("Number of records:", ddl_df_timestamp.count())
    ddl_df_timestamp.show(10,false)


    val stationTo = infer_df
    .select("trip_id", "to_station_name", "gender")
    .where(col("gender") === "Female")
    .groupBy("to_station_name")
    .count()
    .show(10,false)

    spark.stop()
  }
}