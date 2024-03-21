package main.scala.assignment
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import sys._

/**
 * Usage: MnMcount <mnm_file_dataset>
 */
object assignment{
def main(args: Array[String]) {
     val spark = SparkSession.builder.appName("Assignment1").getOrCreate()
     if (args.length < 1) {
     print("Usage: DivvySet <Divvy_file_dataset>") 
     sys.exit(1)
     }

     val Divvy_File = args(0)
     val Infer_DataFrame = spark.read.format("csv") .option("header", "true") .option("inferSchema", "true") .load(Divvy_File)
     Infer_DataFrame.show(20)
     Infer_DataFrame.printSchema()


     val Schema = StructType(Array(StructField("trip_id", IntegerType),
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

     val Struct_DataFrame = spark.read.schema(Schema).format("csv").option("header", "true").option("structureSchema", "true").load(Divvy_File)
     Struct_DataFrame.show(10)
     Struct_DataFrame.printSchema()

// DDl Schema
     val Data_schema = "trip_id INT,starttime STRING,stoptime STRING,bikeid INT,tripduration INT,from_station_id INT,from_station_name STRING,to_station_id INT,to_station_name STRING,usertype STRING,gender STRING,birthyear INT"
     val BlogsDataFrame = (spark.read.schema(Data_schema).format("csv")).option("header", "true").load(Divvy_File)
     BlogsDataFrame.show()
     BlogsDataFrame.printSchema()

     // Transformations and actions
     val Gender_DataFrame=(Infer_DataFrame.select("gender", "to_station_id", "to_station_name").where(Infer_DataFrame.gender == "Female"))
     Gender_DataFrame.groupBy("to_station_id").count().show(10)

}
}