package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object assignment_01 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Assignment1").getOrCreate()
    if (args.length < 1) {
      println("Usage: DivvySet <Divvy_file_dataset>")
      spark.stop()
      sys.exit(1)
    }

    val data_source = args(0)

    // Infer schema
    val infer_divvy_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_source)
    
    //infer_divvy_df.show(10)
    val filtered_df = infer_divvy_df.where(col("gender") === "Female")
    val grouped_df = filtered_df.groupBy("to_station_name").count()
    grouped_df.show(10)
    println("Count: " + infer_divvy_df.count())
    grouped_df.show(10)
    println("Count: " + infer_divvy_df.count())
    println("Schema Inferred Data Frame:")
    infer_divvy_df.printSchema()
    

    // Struct schema
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
    val female_df = struct_divvy_df.where(col("gender") === "Female")
    val fegrouped_df = female_df.groupBy("to_station_name").count()
    // Displaying the DataFrame, printing schema, and counting the number of rows
    println("Grouped by 'to_station_name' where Gender is 'Female':")
    fegrouped_df.show(10)
    println("Count: " + struct_divvy_df.count())
    println("Programmatically Defined Schema:")
    struct_divvy_df.printSchema()
    //println("Count: " + fegrouped_df.count())
    

    // DDL schema
    val ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
    val ddl_df = spark.read.schema(ddl_schema).format("csv")
      .option("header", "true")
      .load(data_source)
    println("DDL Schema Data Frame:")
    val ddfemale_df = ddl_df.where(col("gender") === "Female")
    //ddl_df.show(10)
    val ddlgrouped_df = ddfemale_df.groupBy("to_station_name").count()
    ddlgrouped_df.show(10)
    println("Count: " + ddl_df.count())
    println("DDL Schema Data Frame:")
    ddl_df.printSchema()
    
    spark.stop()
  }
}