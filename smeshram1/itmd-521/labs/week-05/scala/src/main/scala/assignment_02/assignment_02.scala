package main.scala.assignment_02

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// Define DeviceIoTData case class
case class DeviceIoTData(
  battery_level: Long,
  c02_level: Long,
  cca2: String,
  cca3: String,
  cn: String,
  device_id: Long,
  device_name: String,
  humidity: Long,
  ip: String,
  latitude: Double,
  lcd: String,
  longitude: Double,
  scale: String,
  temp: Long,
  timestamp: Long
)
object assignment_02 {
def main(args: Array[String]) {
  val spark = SparkSession
    .builder()
    .appName("DeviceDataFilter")
    .getOrCreate()

if (args.length < 1) {
     print("Usage: IOT device <file>")
     sys.exit(1)
   }
// Get the  file path from command-line argument
val IoTDeviceFile = args(0)
import spark.implicits._
// Data file path
val IoTfilename = "file:///home/vagrant/LearningSparkV2/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json"
// Read JSON file into DataFrame
val ds = spark.read
.json(IoTfilename)
.as[DeviceIoTData]

// Q1.Detect failing devices with battery levels below a threshold.
val batteryThreshold = 1
val failingDevices = ds.filter(_.battery_level < batteryThreshold)
// Count the failing devices
val failingDevicesCount = failingDevices.count()
println(s"Output Q1- Number of devices with battery levels below $batteryThreshold: $failingDevicesCount")
println(s"Details of devices with battery levels below $batteryThreshold:")
failingDevices.show(10,false)

// Output for Question1
// Detecting failing devices with battery levels below a threshold 1: 19851

// Q2.Identify offending countries with high levels of CO2 emissions
val co2Threshold = 1400 
val offendingCountries = ds.groupBy($"cn")
  .agg(avg($"c02_level").alias("avg_co2_level"))
  .filter($"avg_co2_level" > co2Threshold)
  .select($"cn", $"avg_co2_level")
println(s"Output Q2- Countries with average CO2 levels above $co2Threshold:")
offendingCountries.show(false)

// Output for Question2
// Countries with average CO2 levels above the threshold of 1400. It displays the country names along with their corresponding average CO2 levels.
// Countries: Monaco, Gabon, Falkland Islands
//Average CO2 Levels: 1421.5, 1523.0, 1424.0


// Q3. Compute the min and max values for temperature, battery level, CO2, and humidity
val minMaxValues = ds.agg(
  min($"temp").alias("min_temperature"),
  max($"temp").alias("max_temperature"),
  min($"battery_level").alias("min_battery_level"),
  max($"battery_level").alias("max_battery_level"),
  min($"c02_level").alias("min_co2_level"),
  max($"c02_level").alias("max_co2_level"),
  min($"humidity").alias("min_humidity"),
  max($"humidity").alias("max_humidity")
)
println("Output Q3- Min and max values for temperature, battery level, CO2, and humidity:")
minMaxValues.show(false)

// Output for Question4
// The minimum and maximum values for temperature, battery level, CO2 level, and humidity across all devices in the dataset.
// Min Temperature: 10°C
// Max Temperature: 34°C
// Min Battery Level: 0
// Max Battery Level: 9
// Min CO2 Level: 800
// Max CO2 Level: 1599
// Min Humidity: 25
// Max Humidity: 99


// Q4. Sort and group by average temperature, CO2, humidity, and country
val sortedGroupedData =(ds.where(col("cn")=!= "")
  .groupBy($"cn") 
  .agg(avg($"temp").alias("avg_temperature"),
    avg($"c02_level").alias("avg_co2_level"),
    avg($"humidity").alias("avg_humidity"))
  .orderBy($"cn", $"avg_temperature", $"avg_co2_level", $"avg_humidity"))
println("Output Q4- Sorted and grouped data by average temperature, CO2, humidity, and country:")
sortedGroupedData.show(10,false)

// Output for Question5
// The top countries sorted by their average temperature, CO2 level, and humidity. 
// Countries: Afghanistan, Albania, Algeria, American Samoa, Andorra, Angola, Anguilla, Antigua and Barbuda, Argentina, Armenia
// Average Temperature: Ranges from 20.0°C to 31.14°C
// Average CO2 Level: Ranges from 1037.67 ppm to 1279.0 ppm (parts per million)
// Average Humidity: Ranges from 50.71% to 75.0%

// Stop SparkSession
spark.stop()
  }
}