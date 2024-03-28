package main.scala.assignment_02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object assignment_02 {
    def main(args: Array[String]) {

        val spark = SparkSession.builder.appName("iot_devices").getOrCreate()
          
        if (args.length <= 0){
            println("<iot_devices.json>")
            System.exit(1)
        }
    
        // Infer the Schema
        val jsonFile = args(0)
        
        case class DeviceIoTData (
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
            timestamp: Long,
        )

        // Read the file into a Spark DataFrame
        val iot_device_df = spark.read.format("json").option("header", "true").option("inferSchema", "true").load(jsonFile)
        println(iot_device_df.printSchema)

        // Further processing goes here
    }
}

      

        //1. Detect failing devices with battery levels below a threshold.
        val query1 = iot_device_df.select("device_name","battery_level").where("battery_level<2")

        /* Result: 
            24/02/23 05:43:58 INFO CodeGenerator: Code generated in 65.614748 ms
            +--------------------+-------------+
            |         device_name|battery_level|
            +--------------------+-------------+
            |sensor-pad-8xUD6p...|            0|
            |sensor-pad-12Y2kIm0o|            0|
            |sensor-pad-14QL93...|            1|
            |meter-gauge-17zb8...|            0|
            |sensor-pad-36VQv8...|            1|
            | sensor-pad-448DeWGL|            0|
            |meter-gauge-77IKW...|            1|
            |sensor-pad-80TY4d...|            0|
            |sensor-pad-84jla9J5O|            1|
            | therm-stick-85NcuaO|            1|
            |device-mac-87EJxth2l|            1|
            | sensor-pad-92vxuq7e|            0|
            |sensor-pad-98mJQA...|            0|
            |meter-gauge-1075K...|            0|
            |device-mac-111WYt...|            0|
            |meter-gauge-113yf...|            1|
            |therm-stick-115CQ...|            1|
            | sensor-pad-11663yUf|            0|
            |device-mac-117mcc...|            0|
            |meter-gauge-121R9...|            1|
            +--------------------+-------------+
            only showing top 20 rows

            24/02/23 05:43:58 INFO BlockManagerInfo: Removed broadcast_1_piece0 on dp-521:42775 in memory (size: 7.6 KiB, free: 434.3 MiB)

        */

        //2. Identify offending countries with high levels of CO2 emissions.
        val query2 = iot_device_df.groupBy("cn").agg(avg("c02_level").alias("avg_co2_level")).orderBy("avg_co2_level").sort(desc("avg_co2_level"))
        query2.show()

        /* Result:
            24/02/23 05:46:20 INFO CodeGenerator: Code generated in 11.679199 ms
            +----------------+------------------+
            |              cn|     avg_co2_level|
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
            only showing top 20 rows

            24/02/23 05:46:20 INFO FileSourceStrategy: Pushed Filters:

        */

        //3. Compute the min and max values for temperature, battery level, CO2, and humidity.
        val query3 = iot_device_df.select(min("temp").alias("min_temperature"),
        max("temp").alias("max_temperature"),
        min("battery_level").alias("min_batteryLevel"),
        max("battery_level").alias("max_batteryLevel"),
        min("c02_level").alias("min_co2"),
        max("c02_level").alias("max_co2"),
        min("humidity").alias("min_humidity"),
        max("humidity").alias("max_humidity"))
        query3.show()

        /* Result:
            24/02/23 05:46:22 INFO CodeGenerator: Code generated in 17.348445 ms
            +---------------+---------------+----------------+----------------+-------+-------+------------+------------+
            |min_temperature|max_temperature|min_batteryLevel|max_batteryLevel|min_co2|max_co2|min_humidity|max_humidity|
            +---------------+---------------+----------------+----------------+-------+-------+------------+------------+
            |             10|             34|               0|               9|    800|   1599|          25|          99|
            +---------------+---------------+----------------+----------------+-------+-------+------------+------------+

            24/02/23 05:46:22 INFO FileSourceStrategy: Pushed Filters:
        */

        //4. Sort and group by average temperature, CO2, humidity, and country.
        val query4 = iot_device_df.groupBy("cn").agg(avg("temp").alias("avg_temperapure"),avg("c02_level").alias("avg_co2_level"),
        avg("humidity").alias("avg_humidity")).orderBy("avg_temperapure", "avg_co2_level", "avg_humidity").sort(desc("avg_temperapure"))
        query4.show()
        /*
            24/02/23 05:46:24 INFO CodeGenerator: Code generated in 13.449011 ms
            +--------------------+------------------+------------------+------------------+
            |                  cn|   avg_temperapure|     avg_co2_level|      avg_humidity|
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
            |Northern Mariana ...|24.333333333333332| 1164.111111111111|52.333333333333336|
            |          East Timor|24.333333333333332|            1310.0|              59.0|
            |             Bahamas| 24.27777777777778| 1177.388888888889| 68.61111111111111|
            +--------------------+------------------+------------------+------------------+
            only showing top 20 rows
        */
    }
}