package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Assignment1").getOrCreate()
    if (args.length < 1) {
      println("Usage: DivvySet <Divvy_file_dataset>")
      spark.stop()
      return
    }

    val data_source = args(0)

    // Inferred Schema
    val infer_divvy_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_source)

    val filtered_infer_df = infer_divvy_df.where(col("gender") === "Female")
    val grouped_infer_df = filtered_infer_df.groupBy("to_station_name").count()
    println("Inferred Schema - Top 10 Female Destinations:")
    grouped_infer_df.show(10)
    println("Count with Inferred Schema: " + infer_divvy_df.count())
    println("Schema for Inferred Data Frame:")
    infer_divvy_df.printSchema()

    // Programmatically Defined Schema
    val struct_schema = StructType(Array(
      StructField("trip_id", IntegerType),
      StructField("starttime", StringType),
      StructField("stoptime", StringType),
      StructField("bikeid", IntegerType),
      StructField("tripduration", IntegerType),
      StructField("from_station_id", IntegerType),
      StructField("from_station_name", StringType),
      StructField("to_station_id", IntegerType),
      StructField("to_station_name", StringType),
      StructField("usertype", StringType),
      StructField("gender", StringType),
      StructField("birthyear", IntegerType)))

    val struct_divvy_df = spark.read.schema(struct_schema).format("csv")
      .option("header", "true")
      .load(data_source)

    val female_struct_df = struct_divvy_df.where(col("gender") === "Female")
    val grouped_struct_df = female_struct_df.groupBy("to_station_name").count()

    println("Programmatically Defined Schema - Top 10 Female Destinations:")
    grouped_struct_df.show(10)
    println("Count with Programmatically Defined Schema: " + struct_divvy_df.count())
    println("Schema for Programmatically Defined Data Frame:")
    struct_divvy_df.printSchema()

    // DDL String Schema
    val ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val ddl_divvy_df = spark.read.schema(ddl_schema).format("csv")
      .option("header", "true")
      .load(data_source)

    val female_ddl_df = ddl_divvy_df.where(col("gender") === "Female")
    val grouped_ddl_df = female_ddl_df.groupBy("to_station_name").count()

    println("DDL Schema - Top 10 Female Destinations:")
    grouped_ddl_df.show(10)
    println("Count with DDL Schema: " + ddl_divvy_df.count())
    println("Schema for DDL Data Frame:")
    ddl_divvy_df.printSchema()

    spark.stop()
  }
}