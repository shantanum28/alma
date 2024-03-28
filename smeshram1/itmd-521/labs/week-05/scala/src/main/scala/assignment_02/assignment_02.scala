package main.scala.assignment_02
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)


object assignment_02 {
  def main(args: Array[String]) {

    val spark=(SparkSession.builder.appName("assignment02").getOrCreate())
    import spark.implicits._
    if (args.length < 1) {
      println("Usage: Iotset <iot_devices_dataset>")
      spark.stop()
      sys.exit(1)
    }
    val data_source = args(0)
    val iotData = spark.read.json(data_source).as[DeviceIoTData]
    
    //Question1- Detect failing devices with battery levels below a threshold.
    val fail_Devices= iotData.filter(_.battery_level < 5)
    fail_Devices.show(truncate = false)
    
    //For Scala Output comments in 
    /*
+-------------+---------+----+----+-----------------+---------+-----------------------+--------+---------------+--------+------+---------+-------+----+-------------+
|battery_level|c02_level|cca2|cca3|cn               |device_id|device_name            |humidity|ip             |latitude|lcd   |longitude|scale  |temp|timestamp    |
+-------------+---------+----+----+-----------------+---------+-----------------------+--------+---------------+--------+------+---------+-------+----+-------------+
|2            |1556     |IT  |ITA |Italy            |3        |device-mac-36TWSKiT    |44      |88.36.5.1      |42.83   |red   |12.83    |Celsius|19  |1458444054120|
|4            |931      |PH  |PHL |Philippines      |5        |therm-stick-5gimpUrBB  |62      |203.82.41.9    |14.58   |green |120.97   |Celsius|25  |1458444054122|
|3            |1210     |US  |USA |United States    |6        |sensor-pad-6al7RTAobR  |51      |204.116.105.67 |35.93   |yellow|-85.46   |Celsius|27  |1458444054122|
|3            |1129     |CN  |CHN |China            |7        |meter-gauge-7GeDoanM   |26      |220.173.179.1  |22.82   |yellow|108.32   |Celsius|18  |1458444054123|
|0            |1536     |JP  |JPN |Japan            |8        |sensor-pad-8xUD6pzsQI  |35      |210.173.177.1  |35.69   |red   |139.69   |Celsius|27  |1458444054123|
|3            |807      |JP  |JPN |Japan            |9        |device-mac-9GcjZ2pw    |85      |118.23.68.227  |35.69   |green |139.69   |Celsius|13  |1458444054124|
|3            |1544     |IT  |ITA |Italy            |11       |meter-gauge-11dlMTZty  |85      |88.213.191.34  |42.83   |red   |12.83    |Celsius|16  |1458444054125|
|0            |1260     |US  |USA |United States    |12       |sensor-pad-12Y2kIm0o   |92      |68.28.91.22    |38.0    |yellow|-97.0    |Celsius|12  |1458444054126|
|1            |1346     |NO  |NOR |Norway           |14       |sensor-pad-14QL93sBR0j |90      |193.156.90.200 |59.95   |yellow|10.75    |Celsius|16  |1458444054127|
|4            |1425     |US  |USA |United States    |16       |sensor-pad-16aXmIJZtdO |53      |68.85.85.106   |38.0    |red   |-97.0    |Celsius|15  |1458444054128|
|0            |1466     |US  |USA |United States    |17       |meter-gauge-17zb8Fghhl |98      |161.188.212.254|39.95   |red   |-75.16   |Celsius|31  |1458444054129|
|4            |1096     |CN  |CHN |China            |18       |sensor-pad-18XULN9Xv   |25      |221.3.128.242  |25.04   |yellow|102.72   |Celsius|31  |1458444054130|
|4            |880      |US  |USA |United States    |25       |therm-stick-25kK6VyzIFB|78      |24.154.45.90   |41.1    |green |-80.76   |Celsius|27  |1458444054134|
|3            |1502     |KR  |KOR |Republic of Korea|28       |sensor-pad-28Tsudcoikw |64      |211.238.224.77 |37.29   |red   |127.01   |Celsius|25  |1458444054136|
|3            |835      |RU  |RUS |Russia           |33       |device-mac-33B94GfPzi  |30      |178.23.147.134 |55.75   |green |37.62    |Celsius|15  |1458444054139|
|1            |1305     |CY  |CYP |Cyprus           |36       |sensor-pad-36VQv8fnEg  |47      |213.7.14.1     |35.0    |yellow|33.0     |Celsius|24  |1458444054141|
|2            |908      |CN  |CHN |China            |39       |device-mac-39iklYVtvBT |84      |218.7.15.1     |45.75   |green |126.65   |Celsius|17  |1458444054144|
|2            |1224     |FR  |FRA |France           |40       |sensor-pad-40NjeMqS    |27      |188.7.9.1      |49.17   |yellow|6.19     |Celsius|34  |1458444054145|
|2            |913      |ES  |ESP |Spain            |43       |meter-gauge-43RYonsvaW |39      |80.81.64.142   |40.4    |green |-3.68    |Celsius|12  |1458444054148|
|0            |917      |DE  |DEU |Germany          |44       |sensor-pad-448DeWGL    |63      |62.128.16.74   |49.46   |green |11.1     |Celsius|27  |1458444054149|
+-------------+---------+----+----+-----------------+---------+-----------------------+--------+---------------+--------+------+---------+-------+----+-------------+
   */

    //Question2 -Identify offending countries with high levels of CO2 emissions.
    val highCO2Countries = iotData.select($"cn", $"c02_level")
                                  .orderBy(desc("c02_level"))
        

    highCO2Countries.show(truncate = false)

   //  For Scala Output comments in 
    /*
+-------------+---------+
|cn           |c02_level|
+-------------+---------+
|Poland       |1599     |
|Bulgaria     |1599     |
|Spain        |1599     |
|United States|1599     |
|United States|1599     |
|Egypt        |1599     |
|Japan        |1599     |
|Taiwan       |1599     |
|Germany      |1599     |
|Germany      |1599     |
|United States|1599     |
|Germany      |1599     |
|Canada       |1599     |
|Canada       |1599     |
|Philippines  |1599     |
|Japan        |1599     |
|United States|1599     |
|Germany      |1599     |
|Australia    |1599     |
|United States|1599     |
+-------------+---------+ 
   */

    // Question3 - Compute the min and max values for temperature, battery level, CO2, and humidity.
    val minMaxValues = iotData.agg(min("temp").as("min_temp"), max("temp").as("max_temp"),
                                   min("battery_level").as("min_battery_level"), max("battery_level").as("max_battery_level"),
                                   min("c02_level").as("min_CO2_level"), max("c02_level").as("max_CO2_level"),
                                   min("humidity").as("min_humidity"), max("humidity").as("max_humidity"))
    minMaxValues.show(truncate = false)

   //  For Scala Output comments in 
    /*
+--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
|min_temp|max_temp|min_battery_level|max_battery_level|min_CO2_level|max_CO2_level|min_humidity|max_humidity|
+--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
|10      |34      |0                |9                |800          |1599         |25          |99          |
+--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+

  */

    //Question4- Sort and group by average temperature, CO2, humidity, and country.
    val sortedGroupedData = iotData.groupBy("cn")
                                    .agg(avg("temp").as("avg_temp"), avg("c02_level").as("avg_CO2"),
                                         avg("humidity").as("avg_humidity"))
                                    .orderBy($"avg_temp".desc, $"avg_CO2".desc, $"avg_humidity".desc, $"cn")
    sortedGroupedData.show(truncate = false)
   
  //   For Scala Output comments in 
   /*
+------------------------------+------------------+------------------+------------------+
|cn                            |avg_temp          |avg_CO2           |avg_humidity      |
+------------------------------+------------------+------------------+------------------+
|Anguilla                      |31.142857142857142|1165.142857142857 |50.714285714285715|
|Greenland                     |29.5              |1099.5            |56.5              |
|Gabon                         |28.0              |1523.0            |30.0              |
|Vanuatu                       |27.3              |1175.3            |64.0              |
|Saint Lucia                   |27.0              |1201.6666666666667|61.833333333333336|
|Malawi                        |26.666666666666668|1137.0            |59.55555555555556 |
|Turkmenistan                  |26.666666666666668|1093.0            |69.0              |
|Iraq                          |26.428571428571427|1225.5714285714287|62.42857142857143 |
|Laos                          |26.285714285714285|1291.0            |60.857142857142854|
|British Indian Ocean Territory|26.0              |1206.0            |65.0              |
|Cuba                          |25.866666666666667|1222.5333333333333|49.53333333333333 |
|Haiti                         |25.333333333333332|1291.3333333333333|64.58333333333333 |
|Fiji                          |25.09090909090909 |1193.7272727272727|56.45454545454545 |
|Dominica                      |24.73076923076923 |1214.3461538461538|70.46153846153847 |
|Benin                         |24.666666666666668|1038.0            |65.66666666666667 |
|Syria                         |24.6              |1345.8            |57.8              |
|Botswana                      |24.5              |1302.6666666666667|73.75             |
|East Timor                    |24.333333333333332|1310.0            |59.0              |
|Northern Mariana Islands      |24.333333333333332|1164.111111111111 |52.333333333333336|
|Bahamas                       |24.27777777777778 |1177.388888888889 |68.61111111111111 |
+------------------------------+------------------+------------------+------------------+
  */

    spark.stop()
 }
}