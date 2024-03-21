package main.scala.assignment_01

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ScalaDivvyTrips")
      .config("spark.master", "local")
      .getOrCreate()

    val df1 = spark.read.option("header", "true").csv("/home/vagrant/gdhore/itmd-521/labs/week-04/Divvy_Trips_2015-Q1.csv")
    println("First Dataframe - Inferred Schema:")
    df1.printSchema()
    println(s"Total Rows = ${df1.count()}")

    val customSchema = new StructType()
      .add("trip_id", StringType, nullable = true)
      .add("starttime", StringType, nullable = true)
      .add("stoptime", StringType, nullable = true)
      .add("bikeid", StringType, nullable = true)
      .add("tripduration", IntegerType, nullable = true)
      .add("from_station_id", StringType, nullable = true)
      .add("from_station_name", StringType, nullable = true)
      .add("to_station_id", StringType, nullable = true)
      .add("to_station_name", StringType, nullable = true)
      .add("usertype", StringType, nullable = true)
      .add("gender", StringType, nullable = true)
      .add("birthyear", IntegerType, nullable = true)

    val df2 = spark.read.schema(customSchema).option("header", "true").csv("/home/vagrant/gdhore/itmd-521/labs/week-04/Divvy_Trips_2015-Q1.csv")
    println("\nSecond Dataframe - Programmatically Defined Schema:")
    df2.printSchema()
    println(s"Total Rows = ${df2.count()}")

    val ddlSchema = "trip_id STRING, starttime STRING, stoptime STRING, bikeid STRING, tripduration INT, " +
      "from_station_id STRING, from_station_name STRING, to_station_id STRING, to_station_name STRING, " +
      "usertype STRING, gender STRING, birthyear INT"

    val df3 = spark.read.schema(ddlSchema).option("header", "true").csv("/home/vagrant/gdhore/itmd-521/labs/week-04/Divvy_Trips_2015-Q1.csv")
    println("\nThird Dataframe - Schema via DDL:")
    df3.printSchema()
    println(s"Total Rows = ${df3.count()}")

    val df_gender = df3.select("to_station_name", "gender")
      .where(col("gender") === "Female")
      .groupBy("to_station_name")
      .count()
    df_gender.show(10)
  }
}