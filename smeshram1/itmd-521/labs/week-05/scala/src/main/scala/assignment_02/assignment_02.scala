import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: spark-submit --class assignment_02 <jar-file> <file-path>")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName("assignment_02").getOrCreate()

    try {
      import spark.implicits._

      // Load JSON data
      val jsonPath = args(0)
      
      val schema = Encoders.product[DeviceIoTData].schema

      val df = spark.read.schema(schema).json(jsonPath)

      // Question 1: Detect failing devices with battery levels below a threshold.
      // Answer 1:
      println("Question 1:")
      df.filter(col("battery_level") < 5).show(10, truncate = false)

        // +---------+-----------------------+---------------+----+----+-----------------+--------+---------+-------+----+--------+-------------+---------+------+-------------+
        // |device_id|device_name            |ip             |cca2|cca3|cn               |latitude|longitude|scale  |temp|humidity|battery_level|c02_level|lcd   |timestamp    |
        // +---------+-----------------------+---------------+----+----+-----------------+--------+---------+-------+----+--------+-------------+---------+------+-------------+
        // |3        |device-mac-36TWSKiT    |88.36.5.1      |IT  |ITA |Italy            |42.83   |12.83    |Celsius|19  |44      |2            |1556     |red   |1458444054120|
        // |5        |therm-stick-5gimpUrBB  |203.82.41.9    |PH  |PHL |Philippines      |14.58   |120.97   |Celsius|25  |62      |4            |931      |green |1458444054122|
        // |6        |sensor-pad-6al7RTAobR  |204.116.105.67 |US  |USA |United States    |35.93   |-85.46   |Celsius|27  |51      |3            |1210     |yellow|1458444054122|
        // |7        |meter-gauge-7GeDoanM   |220.173.179.1  |CN  |CHN |China            |22.82   |108.32   |Celsius|18  |26      |3            |1129     |yellow|1458444054123|
        // |8        |sensor-pad-8xUD6pzsQI  |210.173.177.1  |JP  |JPN |Japan            |35.69   |139.69   |Celsius|27  |35      |0            |1536     |red   |1458444054123|
        // |9        |device-mac-9GcjZ2pw    |118.23.68.227  |JP  |JPN |Japan            |35.69   |139.69   |Celsius|13  |85      |3            |807      |green |1458444054124|
        // |11       |meter-gauge-11dlMTZty  |88.213.191.34  |IT  |ITA |Italy            |42.83   |12.83    |Celsius|16  |85      |3            |1544     |red   |1458444054125|
        // |12       |sensor-pad-12Y2kIm0o   |68.28.91.22    |US  |USA |United States    |38.0    |-97.0    |Celsius|12  |92      |0            |1260     |yellow|1458444054126|
        // |14       |sensor-pad-14QL93sBR0j |193.156.90.200 |NO  |NOR |Norway           |59.95   |10.75    |Celsius|16  |90      |1            |1346     |yellow|1458444054127|
        // |16       |sensor-pad-16aXmIJZtdO |68.85.85.106   |US  |USA |United States    |38.0    |-97.0    |Celsius|15  |53      |4            |1425     |red   |1458444054128|
        // +---------+-----------------------+---------------+----+----+-----------------+--------+---------+-------+----+--------+-------------+---------+------+-------------+

      // Question 2: Identify offending countries with high levels of CO2 emissions.
      // Answer 2:
      println("Question 2:")
      df.groupBy("cn").agg(avg("c02_level").alias("avg_c02_level")).filter(col("avg_c02_level") > 1000).show(10, truncate = false)

        // +------------------------------+------------------+
        // |cn                            |avg_c02_level     |
        // +------------------------------+------------------+
        // |Russia                        |1202.8179996660544|
        // |Paraguay                      |1212.90625        |
        // |Anguilla                      |1165.142857142857 |
        // |Macao                         |1196.3636363636363|
        // |U.S. Virgin Islands           |1215.9803921568628|
        // |Yemen                         |1182.0            |
        // |British Indian Ocean Territory|1206.0            |
        // |Senegal                       |1140.32           |
        // |Sweden                        |1200.6506944444445|
        // |Republic of Korea             |1196.5763111373012|
        // +------------------------------+------------------+

      // Question 3: Compute the min and max values for temperature, battery level, CO2, and humidity.
      // Answer 3:
      println("Question 3:")
      df.selectExpr("min(temp) as min_temp", "max(temp) as max_temp", "min(battery_level) as min_battery_level", "max(battery_level) as max_battery_level", "min(c02_level) as min_c02_level", "max(c02_level) as max_c02_level", "min(humidity) as min_humidity", "max(humidity) as max_humidity").show(truncate = false)

        // +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
        // |min_temp|max_temp|min_battery_level|max_battery_level|min_c02_level|max_c02_level|min_humidity|max_humidity|
        // +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+
        // |10      |34      |0                |9                |800          |1599         |25          |99          |
        // +--------+--------+-----------------+-----------------+-------------+-------------+------------+------------+


      // Question 4: Sort and group by average temperature, CO2, humidity, and country.
      // Answer 4:
      println("Question 4:")
      df.groupBy("cn").agg(
        avg("temp").alias("avg_temp"),
        avg("c02_level").alias("avg_c02_level"),
        avg("humidity").alias("avg_humidity")
      ).sort(desc("avg_temp")).show(10, truncate = false)

        // +------------------------------+------------------+------------------+------------------+
        // |cn                            |avg_temp          |avg_c02_level     |avg_humidity      |
        // +------------------------------+------------------+------------------+------------------+
        // |Anguilla                      |31.142857142857142|1165.142857142857 |50.714285714285715|
        // |Greenland                     |29.5              |1099.5            |56.5              |
        // |Gabon                         |28.0              |1523.0            |30.0              |
        // |Vanuatu                       |27.3              |1175.3            |64.0              |
        // |Saint Lucia                   |27.0              |1201.6666666666667|61.833333333333336|
        // |Malawi                        |26.666666666666668|1137.0            |59.55555555555556 |
        // |Turkmenistan                  |26.666666666666668|1093.0            |69.0              |
        // |Iraq                          |26.428571428571427|1225.5714285714287|62.42857142857143 |
        // |Laos                          |26.285714285714285|1291.0            |60.857142857142854|
        // |British Indian Ocean Territory|26.0              |1206.0            |65.0              |
        // +------------------------------+------------------+------------------+------------------+

    } finally {
      spark.stop()
    }
  }
}
