package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object assignment_01 {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("assignment_01")
      .getOrCreate()
    
    val ass1_file = args(0)
    
    // Infer schema
    val ass1_infer_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ass1_file)
    val gender_ass1_infer_df = ass1_infer_df.select("*").where(ass1_infer_df("gender") === "Male").groupBy("to_station_name").count()
    val infer_row_count = ass1_infer_df.count()
    
    gender_ass1_infer_df.show(10)
    println(s"Total Rows = ${infer_row_count}")
    println(ass1_infer_df.printSchema)
    println(ass1_infer_df.schema)


    // Programmatic schema
    val ass1_prog_schema = StructType(Array(
      StructField("trip_id", IntegerType, false),
      StructField("starttime", StringType, false),
      StructField("stoptime", StringType, false),
      StructField("bikeid", IntegerType, false),
      StructField("tripduration", IntegerType, false),
      StructField("from_station_id", IntegerType, false),
      StructField("from_station_name", StringType, false),
      StructField("to_station_id", IntegerType, false),
      StructField("to_station_name", StringType, false),
      StructField("usertype", StringType, false),
      StructField("gender", StringType, true),
      StructField("birthyear", IntegerType, true)))
    
    val ass1_prog_df = spark.read.format("csv").option("header", "true").schema(ass1_prog_schema).load(ass1_file)
    val gender_ass1_prog_df = ass1_prog_df.select("*").where(ass1_prog_df("gender") === "Male").groupBy("to_station_name").count()
    val prog_row_count = ass1_prog_df.count()
    
    gender_ass1_prog_df.show(10)
    println(s"Total Rows = ${prog_row_count}")
    println(ass1_prog_df.printSchema)
    println(ass1_prog_df.schema)

    // DDL schema
    val ass1_ddl_schema = "trip_id INTEGER NOT NULL, starttime STRING NOT NULL, stoptime STRING NOT NULL, bikeid INTEGER NOT NULL, tripduration INTEGER NOT NULL, from_station_id INTEGER NOT NULL, from_station_name STRING NOT NULL, to_station_id INTEGER NOT NULL, to_station_name STRING NOT NULL, usertype STRING NOT NULL, gender STRING, birthyear INTEGER"
    val ass1_ddl_df = spark.read.option("header", "true").schema(ass1_ddl_schema).csv(ass1_file)
    val gender_ass1_ddl_df = ass1_ddl_df.select("*").where(ass1_ddl_df("gender") === "Male").groupBy("to_station_name").count()
    val ddl_row_count = ass1_ddl_df.count()
   
    gender_ass1_ddl_df.show(10)
    println(s"Total Rows = ${ddl_row_count}")
    println(ass1_ddl_df.printSchema)
    println(ass1_ddl_df.schema)
  }
}