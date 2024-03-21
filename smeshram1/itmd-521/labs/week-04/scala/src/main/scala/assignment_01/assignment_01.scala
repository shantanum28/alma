package main.scala.assignment_01
import org.apache.spark.sql.functions.avg 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object assignment_01 {
  def main(args: Array[String]): Unit = { 
val spark = SparkSession
      .builder
      .appName("AuthorsAges")
      .getOrCreate()

//Infer schema
val csv_file = args(0)
val sampleDF = spark .read.option("samplingRatio", 0.001).option("header", true) .csv(csv_file)
print(sampleDF.printSchema())
print(sampleDF.count())

//structfields
val schema = StructType(Array(StructField("trip_id", StringType, true),
                            StructField("starttime", StringType, true),
                            StructField("stoptime", StringType, true),
                            StructField("bikeid", StringType, true),
                            StructField("tripduration", StringType, true),
                            StructField("from_station_id", StringType, true),
                            StructField("from_station_name", StringType, true),
                            StructField("to_station_id", StringType, true),
                            StructField("to_station_name", StringType, true),
                            StructField("usertype", StringType, true),
                            StructField("gender", StringType, true),
                            StructField("birthyear", StringType, true)))
val data_frame = spark.read.schema(schema).option("header", "true").csv(csv_file)
print(data_frame.printSchema())
print(data_frame.count())

//ddl
var schema2 = "trip_id STRING, starttime STRING, stoptime STRING, bikeid STRING, tripduration STRING, from_station_id STRING, from_station_name STRING, to_station_id STRING, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"
val data_frame2 = spark.read.schema(schema2).option("header", "true").csv(csv_file)
println(data_frame2.printSchema())
print(data_frame2.count())

//final
val data_frame3 = spark .read.option("samplingRatio", 0.001) .option("header", true) .csv(csv_file)
(data_frame3.select("gender","to_station_name").where(data_frame3("gender")==="Female").groupBy("gender","to_station_name").count().show(10,false))

  }
}