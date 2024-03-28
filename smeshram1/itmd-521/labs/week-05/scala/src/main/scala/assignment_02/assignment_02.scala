package main.scala
import scala.io.StdIn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Lab3{
def main(args: Array[String]) {

// 创建一个 SparkSession
val spark = SparkSession.builder
  .appName("Lab3")
  .getOrCreate()

case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)
//val ds = spark.read.json("./data/iot_devices.json").as[DeviceIoTData]
//val ds  = spark.read.json("./data/iot_devices.json")
//ds.show(5,false)
 val schema = new StructType()
      .add("battery_level", LongType, true)
      .add("c02_level", LongType, true)
      .add("cca2", StringType, true)
      .add("cca3", StringType, true)
      .add("cn", StringType, true)
      .add("device_id", LongType, true)
      .add("device_name", StringType, true)
      .add("humidity", LongType, true)
      .add("ip", StringType, true)
      .add("latitude", DoubleType, true)
      .add("lcd", StringType, true)
      .add("longitude", DoubleType, true)
      .add("scale", StringType, true)
      .add("temp", LongType, true)
      .add("timestamp", LongType, true)
    val df_with_schema = spark.read.schema(schema).json("./data/iot_devices.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)
    //q1
     print("Enter battery_level for the threshold (numbers only) : ")
     val num = StdIn.readInt()
    df_with_schema.select("device_name").where(col("battery_level")< num).show(false)
    //q2
    df_with_schema.select("cn").where(col("c02_level") < 1000).show(false)
    //q3
   // var minMaxRow=df_with_schema.select(min("temp").alias("min_temp"),max("temp").alias("max_temp"),min("battery_level").alias("min_battery"),max("battery_level").alias("max_battery"),min("c02_level").alias("min_c02"),max("c02_level").alias("max_c02"),min("humidity").alias("min_humidity"),max("humidity").alias("max_humidity"))
    //var minMaxRow=df_with_schema.select(min("temp").alias("min_temp"),max("temp").alias("max_temp"))
     //val minValue = minMaxRow.get[Int]("min_temp")
    //val maxValue = minMaxRow.get[Int]("max_temp")
    //println(s"Minimum value: $minValue")
    //println(s"Maximum value: $maxValue")
    println("min temp is :")
     df_with_schema.select("temp").agg(min("temp")).show(false)
    println("max temp is :")
    df_with_schema.select("temp").agg(max("temp")).show(false)  
 println("min battery level is :")
     df_with_schema.select("battery_level").agg(min("battery_level")).show(false)
    println("max battery level is :")
    df_with_schema.select("battery_level").agg(max("battery_level")).show(false)    
 println("min c02 level is :")
     df_with_schema.select("c02_level").agg(min("c02_level")).show(false)
    println("max c02 level is :")
    df_with_schema.select("c02_level").agg(max("c02_level")).show(false)
 println("min humidity is :")
     df_with_schema.select("humidity").agg(min("humidity")).show(false)
    println("max humidity is :")
    df_with_schema.select("humidity").agg(max("humidity")).show(false)
    //q4
    
    val result :DataFrame = df_with_schema.groupBy("cn").agg(avg("temp").alias("avg_temp"),
      avg("c02_level").alias("avg_c02"),
      avg("humidity").alias("avg_humidity"))
    var sortedResult = result.orderBy("cn")
    sortedResult.show(false)

    spark.stop()
}
}