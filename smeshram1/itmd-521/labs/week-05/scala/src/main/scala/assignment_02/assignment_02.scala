package main.scala.assignment_02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class DeviceIoTData1 (battery_level: Long, c02_level: Long,
                           cca2: String, cca3: String, cn: String, device_id: Long,
                           device_name: String, humidity: Long, ip: String, latitude: Double,
                           lcd: String, longitude: Double, scale:String, temp: Long,
                           timestamp: Long)

object assignment_02 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("assignment_02").getOrCreate()
    import spark.implicits._

    if (args.length < 1) {
      println("Usage: assignment_02 <iot_devices_dataset>")
      spark.stop()
      sys.exit(1)
    }

    try {
      // Load IoT data from JSON file
      val datasource = args(0)
      val iot_data = spark.read.json(datasource).as[DeviceIoTData1]

      // Task 1: Detect failing devices with battery levels below a threshold
      val failing_Devices = iot_data.filter($"battery_level" < 8)
 /*
  +-------------+---------+----+----+-------------+---------+--------------------+--------+---------------+--------+------+---------+-------+----+-------------+
|battery_level|c02_level|cca2|cca3|           cn|device_id|         device_name|humidity|             ip|latitude|   lcd|longitude|  scale|temp|    timestamp|
+-------------+---------+----+----+-------------+---------+--------------------+--------+---------------+--------+------+---------+-------+----+-------------+
|            7|     1473|  NO| NOR|       Norway|        2|   sensor-pad-2n2Pea|      70|  213.161.254.1|   62.47|   red|     6.15|Celsius|  11|1458444054119|
|            2|     1556|  IT| ITA|        Italy|        3| device-mac-36TWSKiT|      44|      88.36.5.1|   42.83|   red|    12.83|Celsius|  19|1458444054120|
|            6|     1080|  US| USA|United States|        4|   sensor-pad-4mzWkz|      32|  66.39.173.154|   44.06|yellow|  -121.32|Celsius|  28|1458444054121|
|            4|      931|  PH| PHL|  Philippines|        5|therm-stick-5gimp...|      62|    203.82.41.9|   14.58| green|   120.97|Celsius|  25|1458444054122|
|            3|     1210|  US| USA|United States|        6|sensor-pad-6al7RT...|      51| 204.116.105.67|   35.93|yellow|   -85.46|Celsius|  27|1458444054122|
|            3|     1129|  CN| CHN|        China|        7|meter-gauge-7GeDoanM|      26|  220.173.179.1|   22.82|yellow|   108.32|Celsius|  18|1458444054123|
|            0|     1536|  JP| JPN|        Japan|        8|sensor-pad-8xUD6p...|      35|  210.173.177.1|   35.69|   red|   139.69|Celsius|  27|1458444054123|
|            3|      807|  JP| JPN|        Japan|        9| device-mac-9GcjZ2pw|      85|  118.23.68.227|   35.69| green|   139.69|Celsius|  13|1458444054124|
|            7|     1470|  US| USA|United States|       10|sensor-pad-10Bsyw...|      56|208.109.163.218|   33.61|   red|  -111.89|Celsius|  26|1458444054125|
|            3|     1544|  IT| ITA|        Italy|       11|meter-gauge-11dlM...|      85|  88.213.191.34|   42.83|   red|    12.83|Celsius|  16|1458444054125|
|            0|     1260|  US| USA|United States|       12|sensor-pad-12Y2kIm0o|      92|    68.28.91.22|    38.0|yellow|    -97.0|Celsius|  12|1458444054126|
|            6|     1007|  IN| IND|        India|       13|meter-gauge-13Gro...|      92| 59.144.114.250|    28.6|yellow|     77.2|Celsius|  13|1458444054127|
|            1|     1346|  NO| NOR|       Norway|       14|sensor-pad-14QL93...|      90| 193.156.90.200|   59.95|yellow|    10.75|Celsius|  16|1458444054127|
|            4|     1425|  US| USA|United States|       16|sensor-pad-16aXmI...|      53|   68.85.85.106|    38.0|   red|    -97.0|Celsius|  15|1458444054128|
|            0|     1466|  US| USA|United States|       17|meter-gauge-17zb8...|      98|161.188.212.254|   39.95|   red|   -75.16|Celsius|  31|1458444054129|
|            4|     1096|  CN| CHN|        China|       18|sensor-pad-18XULN9Xv|      25|  221.3.128.242|   25.04|yellow|   102.72|Celsius|  31|1458444054130|
|            7|     1155|  US| USA|United States|       20|sensor-pad-20gFNf...|      33|  66.153.162.66|   33.94|yellow|   -78.92|Celsius|  10|1458444054131|
|            5|      939|  AT| AUT|      Austria|       21|  device-mac-21sjz5h|      44|193.200.142.254|    48.2| green|    16.37|Celsius|  30|1458444054131|
|            7|     1522|  JP| JPN|        Japan|       22|  sensor-pad-22oWV2D|      58| 221.113.129.83|   35.69|   red|   139.69|Celsius|  24|1458444054132|
|            5|     1245|  IN| IND|        India|       23| meter-gauge-230IupA|      47|     59.90.65.1|   12.98|yellow|    77.58|Celsius|  23|1458444054133|
+-------------+---------+----+----+-------------+---------+--------------------+--------+---------------+--------+------+---------+-------+----+-------------+
only showing top 20 rows */
      // Task 2: Identify offending countries with high levels of CO2 emissions
      val offend_Countries_co2_Emission= iot_data.groupBy("cn").agg(avg("c02_level").as("avgCO2")).orderBy(desc("avgCO2"))
        

    /*
    +----------------+------------------+
|              cn|            avgCO2|
+----------------+------------------+
|           Gabon|            1523.0|
|Falkland Islands|            1424.0|
|          Monaco|            1421.5|
|          Kosovo|            1389.0|
|      San Marino|1379.6666666666667|
|         Liberia|            1374.5|
|           Syria|            1345.8|
|      Mauritania|1344.4285714285713|
|           Congo|          1333.375|
|           Tonga|            1323.0|
|      East Timor|            1310.0|
|          Guinea|            1308.0|
|        Botswana|1302.6666666666667|
|           Haiti|1291.3333333333333|
|            Laos|            1291.0|
|        Maldives|1284.7272727272727|
|    Sint Maarten|1282.2857142857142|
|         Andorra|            1279.0|
|         Lesotho|            1274.6|
|      Mozambique|            1264.0|
+----------------+------------------+
        only showing top 20 rows */

      // Task 3: Compute the min and max values for temperature, battery level, CO2, and humidity
      val aggregated_values= iot_data.agg(
        min("temp").as("minTemperature"),
        max("temp").as("maxTemperature"),
        min("battery_level").as("minBatteryLevel"),
        max("battery_level").as("maxBatteryLevel"),
        min("c02_level").as("minCO2"),
        max("c02_level").as("maxCO2"),
        min("humidity").as("minHumidity"),
        max("humidity").as("maxHumidity")
      )

  /* +--------------+--------------+---------------+---------------+------+------+-----------+-----------+
    |minTemperature|maxTemperature|minBatteryLevel|maxBatteryLevel|minCO2|maxCO2|minHumidity|maxHumidity|
    +--------------+--------------+---------------+---------------+------+------+-----------+-----------+
    |            10|            34|              0|              9|   800|  1599|         25|         99|
    +--------------+--------------+---------------+---------------+------+------+-----------+-----------+"""
*/
      // Task 4: Sort and group by average temperature, CO2, humidity, and country
      val grouped_avg= iot_data.groupBy("cn")
        .agg(
          avg("temp").as("avgTemperature"),
          avg("c02_level").as("avgCO2"),
          avg("humidity").as("avgHumidity")
        )
        .orderBy(desc("avgTemperature"), desc("avgCO2"), desc("avgHumidity"), asc("cn"))
  /*
  +--------------------+------------------+------------------+------------------+
  |                  cn|    avgTemperature|            avgCO2|       avgHumidity|
  +--------------------+------------------+------------------+------------------+
  |            Anguilla|31.142857142857142| 1165.142857142857|50.714285714285715|
  |           Greenland|              29.5|            1099.5|              56.5|
  |               Gabon|              28.0|            1523.0|              30.0|
  |             Vanuatu|              27.3|            1175.3|              64.0|
  |         Saint Lucia|              27.0|1201.6666666666667|61.833333333333336|
  |              Malawi|26.666666666666668|            1137.0| 59.55555555555556|
  |        Turkmenistan|26.666666666666668|            1093.0|              69.0|
  |                Iraq|26.428571428571427|1225.5714285714287| 62.42857142857143|
  |                Laos|26.285714285714285|            1291.0|60.857142857142854|
  |British Indian Oc...|              26.0|            1206.0|              65.0|
  |                Cuba|25.866666666666667|1222.5333333333333| 49.53333333333333|
  |               Haiti|25.333333333333332|1291.3333333333333| 64.58333333333333|
  |                Fiji| 25.09090909090909|1193.7272727272727| 56.45454545454545|
  |            Dominica| 24.73076923076923|1214.3461538461538| 70.46153846153847|
  |               Benin|24.666666666666668|            1038.0| 65.66666666666667|
  |               Syria|              24.6|            1345.8|              57.8|
  |            Botswana|              24.5|1302.6666666666667|             73.75|
  |          East Timor|24.333333333333332|            1310.0|              59.0|
  |Northern Mariana ...|24.333333333333332| 1164.111111111111|52.333333333333336|
  |             Bahamas| 24.27777777777778| 1177.388888888889| 68.61111111111111|
  +--------------------+------------------+------------------+------------------+
  only showing top 20 rows"""
*/
      // Show  the results of iotdata
      failing_Devices.show()
      offend_Countries_co2_Emission.show()
      aggregated_values.show()
      grouped_avg.show()

    } catch {
      case e: Exception =>
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}