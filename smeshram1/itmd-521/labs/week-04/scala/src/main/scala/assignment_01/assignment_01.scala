package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Assignment1").getOrCreate()

    if (args.length < 1) {
      println("Usage: DivvySet <Divvy_file_dataset>")
      spark.stop()
      sys.exit(1)
    }

    val data_source = args(0)

    // Read CSV with inferred schema
    val infer_divvy_df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_source)
    printDataFrameInfo("Schema Inferred Data Frame:", infer_divvy_df)

    // Define StructType schema
    val struct_schema = StructType(
      Seq("trip_id", "bikeid", "tripduration", "from_station_id", "to_station_id", "birthyear")
        .map(field => StructField(field, IntegerType)) ++
        Seq("starttime", "stoptime", "from_station_name", "to_station_name", "usertype", "gender")
          .map(field => StructField(field, StringType))
    )

    // Read CSV with StructType schema
    val struct_divvy_df = spark.read.schema(struct_schema).option("header", "true").csv(data_source)
    printDataFrameInfo("Programmatically Defined Schema:", struct_divvy_df)

    // Define DDL schema
    val ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    // Read CSV with DDL schema
    val ddl_df = spark.read.schema(ddl_schema).option("header", "true").csv(data_source)
    printDataFrameInfo("DDL Schema Data Frame:", ddl_df)

    // Filter and group by gender
    val female_to_station_df = infer_divvy_df.filter("gender = 'Female'").groupBy("to_station_name").count()
    printDataFrameInfo("Female Gender Data Frame grouped by to_station_name:", female_to_station_df, Some(10))

    spark.stop()
  }

  // Helper function to print DataFrame information
  def printDataFrameInfo(title: String, df: org.apache.spark.sql.DataFrame, numRows: Option[Int] = None): Unit = {
    println(title)
    df.printSchema()
    println(s"Count: ${df.count()}")
    numRows.foreach(n => df.show(n))
    println()
  }
}