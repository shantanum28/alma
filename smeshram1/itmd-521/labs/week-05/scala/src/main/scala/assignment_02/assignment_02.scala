package main.scala.assignment_02
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._


case class IotDeviceData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)


object assignment_02 {
  def main(args: Array[String]) {

    val spark=(SparkSession.builder.appName("assignment02").getOrCreate())
    import spark.implicits._
    if (args.length < 1) {
      println("Usage: Iotset <iot_dataset>")
      spark.stop()
      sys.exit(1)
    }
    val data_source = args(0)
    val iotData = spark.read.json(data_source).as[IotDeviceData]
    
    //q1- Detect failing devices with batteries levels below a threshold.
    val df_q1= iotData.filter(_.battery_level < 2)
    df_q1.show(truncate = false)
    /*
    +-------------+---------+----+----+-----------------+---------+------------------------+--------+---------------+--------+------+---------+-------+----+-------------+
|battery_level|c02_level|cca2|cca3|cn               |device_id|device_name             |humidity|ip             |latitude|lcd   |longitude|scale  |temp|timestamp    |
+-------------+---------+----+----+-----------------+---------+------------------------+--------+---------------+--------+------+---------+-------+----+-------------+
|0            |1536     |JP  |JPN |Japan            |8        |sensor-pad-8xUD6pzsQI   |35      |210.173.177.1  |35.69   |red   |139.69   |Celsius|27  |1458444054123|
|0            |1260     |US  |USA |United States    |12       |sensor-pad-12Y2kIm0o    |92      |68.28.91.22    |38.0    |yellow|-97.0    |Celsius|12  |1458444054126|
|1            |1346     |NO  |NOR |Norway           |14       |sensor-pad-14QL93sBR0j  |90      |193.156.90.200 |59.95   |yellow|10.75    |Celsius|16  |1458444054127|
|0            |1466     |US  |USA |United States    |17       |meter-gauge-17zb8Fghhl  |98      |161.188.212.254|39.95   |red   |-75.16   |Celsius|31  |1458444054129|
|1            |1305     |CY  |CYP |Cyprus           |36       |sensor-pad-36VQv8fnEg   |47      |213.7.14.1     |35.0    |yellow|33.0     |Celsius|24  |1458444054141|
|0            |917      |DE  |DEU |Germany          |44       |sensor-pad-448DeWGL     |63      |62.128.16.74   |49.46   |green |11.1     |Celsius|27  |1458444054149|
|1            |1454     |IN  |IND |India            |77       |meter-gauge-77IKW3YAB55 |82      |218.248.255.30 |12.98   |red   |77.58    |Celsius|17  |1458444054169|
|0            |1233     |CA  |CAN |Canada           |80       |sensor-pad-80TY4dWSMH   |57      |159.128.0.181  |50.01   |yellow|-97.22   |Celsius|32  |1458444054171|
|1            |941      |JP  |JPN |Japan            |84       |sensor-pad-84jla9J5O    |31      |122.133.0.26   |35.69   |green |139.69   |Celsius|27  |1458444054174|
|1            |1028     |US  |USA |United States    |85       |therm-stick-85NcuaO     |88      |205.213.119.42 |43.06   |yellow|-89.41   |Celsius|17  |1458444054174|
|1            |1212     |US  |USA |United States    |87       |device-mac-87EJxth2l    |38      |204.16.48.130  |33.42   |yellow|-86.68   |Celsius|25  |1458444054175|
|0            |1021     |KR  |KOR |Republic of Korea|92       |sensor-pad-92vxuq7e     |52      |211.195.95.61  |37.57   |yellow|126.98   |Celsius|28  |1458444054178|
|0            |952      |US  |USA |United States    |98       |sensor-pad-98mJQAfJpfW  |50      |12.116.241.94  |38.0    |green |-97.0    |Celsius|22  |1458444054181|
|0            |1553     |JP  |JPN |Japan            |107      |meter-gauge-1075KSUDRjPa|93      |163.139.227.70 |35.69   |red   |139.69   |Celsius|22  |1458444054186|
|0            |899      |AU  |AUS |Australia        |111      |device-mac-111WYtjxe1b  |32      |203.123.94.193 |-27.0   |green |133.0    |Celsius|16  |1458444054189|
|1            |1163     |SG  |SGP |Singapore        |113      |meter-gauge-113yfSV5qK  |32      |210.193.58.1   |1.29    |yellow|103.86   |Celsius|18  |1458444054191|
|1            |876      |CZ  |CZE |Czech Republic   |115      |therm-stick-115CQYYpj   |85      |217.198.117.2  |50.08   |green |14.42    |Celsius|27  |1458444054192|
|0            |1480     |US  |USA |United States    |116      |sensor-pad-11663yUf     |77      |65.40.86.249   |38.0    |red   |-97.0    |Celsius|19  |1458444054193|
|0            |1538     |CN  |CHN |China            |117      |device-mac-117mccYyRo   |41      |211.137.250.166|35.0    |red   |105.0    |Celsius|29  |1458444054194|
|1            |1360     |US  |USA |United States    |121      |meter-gauge-121R9rukaY2 |91      |68.85.158.77   |38.0    |yellow|-97.0    |Celsius|24  |1458444054198|
+-------------+---------+----+----+-----------------+---------+------------------------+--------+---------------+--------+------+---------+-------+----+-------------+
    */

    //q2 -Identify offending countries with high levels of CO2 emissions.
    val df_q2 = iotData.groupBy("cn")
                              .agg(avg("c02_level").as("avg_CO2_emmision"))
                              .orderBy(desc("avg_CO2_emmision"))
    
    df_q2.show(truncate = false)
    /*
    +----------------+------------------+
|cn              |avg_CO2_emmision  |
+----------------+------------------+
|Gabon           |1523.0            |
|Falkland Islands|1424.0            |
|Monaco          |1421.5            |
|Kosovo          |1389.0            |
|San Marino      |1379.6666666666667|
|Liberia         |1374.5            |
|Syria           |1345.8            |
|Mauritania      |1344.4285714285713|
|Congo           |1333.375          |
|Tonga           |1323.0            |
|East Timor      |1310.0            |
|Guinea          |1308.0            |
|Botswana        |1302.6666666666667|
|Haiti           |1291.3333333333333|
|Laos            |1291.0            |
|Maldives        |1284.7272727272727|
|Sint Maarten    |1282.2857142857142|
|Andorra         |1279.0            |
|Lesotho         |1274.6            |
|Mozambique      |1264.0            |
+----------------+------------------+
only showing top 20 rows
*/

    // q3 - Compute the min and max values for temperature, battery level, CO2, and humidity.
    val df_q3 = iotData.agg(min("temp").as("min_temp"), max("temp").as("max_temp"),
                                   min("battery_level").as("min_battery_level"), max("battery_level").as("max_battery_level"),
                                   min("c02_level").as("min_CO2_level"), max("c02_level").as("max_CO2_level"),
                                   min("humidity").as("min_humidity"), max("humidity").as("max_humidity"))
    df_q3.show(truncate = false)

    /*
    +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
|min_temp|max_temp|min_battery_level|max_battery_level|min_CO2_level|max_CO2_level|min_humidity|max_humidity|
+--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
|10      |34      |0                |9                |800          |1599         |25          |99          |
+--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+

*/

    //q4 -q4 - Sort and group by average temperature, CO2, humidity, and country.
    val df_q4 = iotData.groupBy("cn")
                                    .agg(avg("temp").as("avg_temp"), avg("c02_level").as("avg_CO2"),
                                         avg("humidity").as("avg_humid"))
                                    .orderBy($"avg_temp".desc, $"avg_CO2".desc, $"avg_humid".desc, $"cn")
    df_q4.show(truncate = false)

    /*
    +------------------------------+------------------+------------------+------------------+
|cn                            |avg_temp          |avg_CO2           |avg_humid         |
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
only showing top 20 rows
*/
   
    
    spark.stop()
 }
}