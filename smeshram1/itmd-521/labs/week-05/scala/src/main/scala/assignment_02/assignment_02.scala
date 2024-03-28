package main.scala.assignment_02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object assignment_02 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: assignment_02 <file>")
      sys.exit(1)
    }

    //Initialize Spark Session
    val spark = SparkSession.builder.appName("IoTDevicesAnalysis_Scala").getOrCreate()

    // Define the fields for the schema
    val fields = Array(
    StructField("device_id", IntegerType, nullable = true),
    StructField("device_name", StringType, nullable = true),
    StructField("ip", StringType, nullable = true),
    StructField("cca2", StringType, nullable = true),
    StructField("cca3", StringType, nullable = true),
    StructField("cn", StringType, nullable = true),
    StructField("latitude", DoubleType, nullable = true),
    StructField("longitude", DoubleType, nullable = true),
    StructField("scale", StringType, nullable = true),
    StructField("temp", IntegerType, nullable = true),
    StructField("humidity", IntegerType, nullable = true),
    StructField("battery_level", IntegerType, nullable = true),
    StructField("c02_level", IntegerType, nullable = true),
    StructField("lcd", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
    StructField("event_type", StringType, nullable = true),
    StructField("event_value", IntegerType, nullable = true),
    StructField("event_unit", StringType, nullable = true)
    )

    // Create the StructType with the defined StructFields
    val iotDevices_Schema = StructType(fields)

    // using this schema to read the data into a DataFrame
    val iotDevices_DF = spark.read.schema(iotDevices_Schema).json("file:///home/vagrant/LearningSparkV2/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")

    // Print the schema of the DataFrame
    println("Schema of the iotDevices DataFrame:")
    iotDevices_DF.printSchema()

    // Performing Spark operations for below 4 questions 

    //1. Detect failing devices with battery levels below a threshold.
    println("Failing devices with battery levels below 20:")

    val failingDevices_below_threshold = iotDevices_DF.filter(col("battery_level") < 20)
    failingDevices_below_threshold.show(10, truncate=false)

    /*
    Failing devices with battery levels below 20:
    +---------+---------------------+---------------+----+----+-------------+--------+---------+-------+----+--------+-------------+---------+------+-------------+----------+-----------+----------+
    |device_id|device_name          |ip             |cca2|cca3|cn           |latitude|longitude|scale  |temp|humidity|battery_level|c02_level|lcd   |timestamp    |event_type|event_value|event_unit|
    +---------+---------------------+---------------+----+----+-------------+--------+---------+-------+----+--------+-------------+---------+------+-------------+----------+-----------+----------+
    |1        |meter-gauge-1xbYRYcj |68.161.225.1   |US  |USA |United States|38.0    |-97.0    |Celsius|34  |51      |8            |868      |green |1458444054093|NULL      |NULL       |NULL      |
    |2        |sensor-pad-2n2Pea    |213.161.254.1  |NO  |NOR |Norway       |62.47   |6.15     |Celsius|11  |70      |7            |1473     |red   |1458444054119|NULL      |NULL       |NULL      |
    |3        |device-mac-36TWSKiT  |88.36.5.1      |IT  |ITA |Italy        |42.83   |12.83    |Celsius|19  |44      |2            |1556     |red   |1458444054120|NULL      |NULL       |NULL      |
    |4        |sensor-pad-4mzWkz    |66.39.173.154  |US  |USA |United States|44.06   |-121.32  |Celsius|28  |32      |6            |1080     |yellow|1458444054121|NULL      |NULL       |NULL      |
    |5        |therm-stick-5gimpUrBB|203.82.41.9    |PH  |PHL |Philippines  |14.58   |120.97   |Celsius|25  |62      |4            |931      |green |1458444054122|NULL      |NULL       |NULL      |
    |6        |sensor-pad-6al7RTAobR|204.116.105.67 |US  |USA |United States|35.93   |-85.46   |Celsius|27  |51      |3            |1210     |yellow|1458444054122|NULL      |NULL       |NULL      |
    |7        |meter-gauge-7GeDoanM |220.173.179.1  |CN  |CHN |China        |22.82   |108.32   |Celsius|18  |26      |3            |1129     |yellow|1458444054123|NULL      |NULL       |NULL      |
    |8        |sensor-pad-8xUD6pzsQI|210.173.177.1  |JP  |JPN |Japan        |35.69   |139.69   |Celsius|27  |35      |0            |1536     |red   |1458444054123|NULL      |NULL       |NULL      |
    |9        |device-mac-9GcjZ2pw  |118.23.68.227  |JP  |JPN |Japan        |35.69   |139.69   |Celsius|13  |85      |3            |807      |green |1458444054124|NULL      |NULL       |NULL      |
    |10       |sensor-pad-10BsywSYUF|208.109.163.218|US  |USA |United States|33.61   |-111.89  |Celsius|26  |56      |7            |1470     |red   |1458444054125|NULL      |NULL       |NULL      |
    +---------+---------------------+---------------+----+----+-------------+--------+---------+-------+----+--------+-------------+---------+------+-------------+----------+-----------+----------+
    only showing top 10 rows
    */

    //2. Identify offending countries with high levels of CO2 emissions.
   
    // Group the data by country code and calculate total CO2 emissions for each country
    val co2EmissionsByCountry = iotDevices_DF.groupBy("cca3").agg(sum("c02_level").alias("total_co2_emissions"))

    // Define the threshold for high CO2 emissions
    val highCO2Threshold = 1000

    // Filter for countries with high levels of CO2 emissions
    val high_CO2_offendingCountries = co2EmissionsByCountry.filter(col("total_co2_emissions") > highCO2Threshold).orderBy(col("total_co2_emissions").desc)

    // Show the offending countries
    println("Offending countries with high levels of CO2 emissions:")
    high_CO2_offendingCountries.show(10, truncate=false)

    /*
    Offending countries with high levels of CO2 emissions:
    +----+-------------------+
    |cca3|total_co2_emissions|
    +----+-------------------+
    |USA |84493636           |
    |CHN |17349538           |
    |JPN |14479050           |
    |KOR |14214130           |
    |DEU |9526623            |
    |GBR |7799008            |
    |CAN |7268528            |
    |RUS |7203677            |
    |FRA |6369745            |
    |BRA |3896972            |
    +----+-------------------+
    only showing top 10 rows

    */

    //3. Compute the min and max values for temperature, battery level, CO2, and humidity.

    // Compute the min and max values for temperature, battery level, CO2, and humidity
    val minMaxValues_temp_battery_CO2_humidity = iotDevices_DF.agg(
    min("temp").alias("min_temperature"),max("temp").alias("max_temperature"),
    min("battery_level").alias("min_battery_level"),max("battery_level").alias("max_battery_level"),
    min("c02_level").alias("min_c02_level"),max("c02_level").alias("max_c02_level"),
    min("humidity").alias("min_humidity"),max("humidity").alias("max_humidity"))

    // Show the computed min and max values
    println("Minimum and maximum values for temperature, battery level, CO2, and humidity:")
    minMaxValues_temp_battery_CO2_humidity.show()

    /*

    Minimum and maximum values for temperature, battery level, CO2, and humidity:
    +---------------+---------------+-----------------+-----------------+-------------+-------------+------------+------------+
    |min_temperature|max_temperature|min_battery_level|max_battery_level|min_c02_level|max_c02_level|min_humidity|max_humidity|
    +---------------+---------------+-----------------+-----------------+-------------+-------------+------------+------------+
    |             10|             34|                0|                9|          800|         1599|          25|          99|
    +---------------+---------------+-----------------+-----------------+-------------+-------------+------------+------------+

    */

    //4. Sort and group by average temperature, CO2, humidity, and country.

    // Group the data by country code and calculate average temperature, CO2, and humidity for each country
    val avgValuesByCountry = iotDevices_DF.groupBy("cn").agg(
    round(avg("temp"),2).alias("avg_temperature"),
    round(avg("c02_level"),2).alias("avg_c02_level"),
    round(avg("humidity"),2).alias("avg_humidity")).orderBy(asc("cn"))

    // Sort the resulting DataFrame by the average values of temperature, CO2, and humidity
    val sortedAvgValuesByCountry = avgValuesByCountry.sort(
    col("avg_temperature").desc,col("avg_c02_level").desc,col("avg_humidity").desc)

    // Show the sorted and grouped data
    println("Sorted and grouped by average temperature, CO2, humidity, and country:")
    sortedAvgValuesByCountry.show(10, truncate=false)

    /*

    +------------------------------+---------------+-------------+------------+
    |cn                            |avg_temperature|avg_c02_level|avg_humidity|
    +------------------------------+---------------+-------------+------------+
    |Anguilla                      |31.14          |1165.14      |50.71       |
    |Greenland                     |29.5           |1099.5       |56.5        |
    |Gabon                         |28.0           |1523.0       |30.0        |
    |Vanuatu                       |27.3           |1175.3       |64.0        |
    |Saint Lucia                   |27.0           |1201.67      |61.83       |
    |Malawi                        |26.67          |1137.0       |59.56       |
    |Turkmenistan                  |26.67          |1093.0       |69.0        |
    |Iraq                          |26.43          |1225.57      |62.43       |
    |Laos                          |26.29          |1291.0       |60.86       |
    |British Indian Ocean Territory|26.0           |1206.0       |65.0        |
    +------------------------------+---------------+-------------+------------+
    only showing top 10 rows

    */

    // Step 7: Stop SparkSession
    spark.stop()

  }
}