from os import truncate
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, desc, month, to_date, year, avg, weekofyear,to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Firecsv <file>", file=sys.stderr)
        sys.exit(-1)

    # Create SparkSession
    spark = SparkSession.builder.appName("Assignment 2").getOrCreate()
    data_source = sys.argv[1]


    schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField("IncidentNumber", IntegerType(), True),
    StructField("CallType", StringType(), True),
    StructField("CallDate", StringType(), True),
    StructField("WatchDate", StringType(), True),
    StructField("CallFinalDisposition", StringType(), True),
    StructField("AvailableDtTm", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("Battalion", StringType(), True),
    StructField("StationArea", IntegerType(), True),
    StructField("Box", IntegerType(), True),
    StructField("OriginalPriority", IntegerType(), True),
    StructField("Priority", IntegerType(), True),
    StructField("FinalPriority", IntegerType(), True),
    StructField("ALSUnit", BooleanType(), True),
    StructField("CallTypeGroup", StringType(), True),
    StructField("NumAlarms", IntegerType(), True),
    StructField("UnitType", StringType(), True),
    StructField("UnitSequenceInCallDispatch", IntegerType(), True),
    StructField("FirePreventionDistrict", IntegerType(), True),
    StructField("SupervisorDistrict", IntegerType(), True),
    StructField("Neighborhood", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("RowID", StringType(), True),
    StructField("Delay", DoubleType(), True)
])
    
    firecall_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_source)

    # Q1 - What were all the different types of fire calls in 2018?
    fire_calls = firecall_df.filter((col("CallDate").like("%2018%")))
    q1_df = fire_calls.select("CallType").distinct()
    q1_df.show(10,truncate = False)

    '''

    +-------------------------------+
|CallType                       |
+-------------------------------+
|Elevator / Escalator Rescue    |
|Alarms                         |
|Odor (Strange / Unknown)       |
|Citizen Assist / Service Call  |
|HazMat                         |
|Vehicle Fire                   |
|Other                          |
|Outside Fire                   |
|Traffic Collision              |
|Assist Police                  |
|Gas Leak (Natural and LP Gases)|
|Water Rescue                   |
|Electrical Hazard              |
|Structure Fire                 |
|Medical Incident               |
|Fuel Spill                     |
|Smoke Investigation (Outside)  |
|Train / Rail Incident          |
|Explosion                      |
|Suspicious Package             |
+-------------------------------+
'''

    # Q2 - What months within the year 2018 saw the highest number of fire calls?
    
    firecall_2018 = firecall_df.filter(col("CallDate").like("%2018%") & col("CallDate").isNotNull())
    firecall_2018 = firecall_2018.withColumn("CallDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    firecall_2018 = firecall_2018.withColumn("Month", month("CallDate"))
    q2_df = firecall_2018.groupBy("Month").count().orderBy(desc("count"))
    q2_df.show(12,truncate = False)

    '''

+-----+-----+
|Month|count|
+-----+-----+
|10   |1068 |
|5    |1047 |
|3    |1029 |
|8    |1021 |
|1    |1007 |
|6    |974  |
|7    |974  |
|9    |951  |
|4    |947  |
|2    |919  |
|11   |199  |
+-----+-----+
    '''

    

    #q3 - Which neighborhood in San Francisco generated the most fire calls in 2018?
    # sf_fire_calls = fire_calls.filter(col("City").isin("San Francisco", "SF","SFO"))
    sf_fire_calls = fire_calls.filter(col("City") == "San Francisco")
    q3_df = sf_fire_calls.groupBy("Neighborhood").agg(count("*").alias("TotalCalls")).orderBy(desc("TotalCalls"))
    q3_df.show(3,truncate = False)

    '''
    +---------------+----------+
|Neighborhood   |TotalCalls|
+---------------+----------+
|Tenderloin     |1393      |
|South of Market|1053      |
|Mission        |913       |
+---------------+----------+
only showing top 3 rows
    
    '''

    #Q4 - Which neighborhoods had the worst response times to fire calls in 2018?
    fire_call_2018 = firecall_df.filter((col("CallDate").like("%2018%"))& (col("CallDate").isNotNull()))
    n_res = fire_call_2018.groupBy("Neighborhood").agg(avg("Delay").alias("AverageDelay"))
    q4_df = n_res.orderBy(desc("AverageDelay"))
    q4_df.show()

    '''
    +--------------------+------------------+
|        Neighborhood|      AverageDelay|
+--------------------+------------------+
|           Chinatown|6.1903140979057625|
|            Presidio| 5.829227041449275|
|     Treasure Island| 5.453703712499999|
|        McLaren Park| 4.744047642857142|
|Bayview Hunters P...|4.6205619568773955|
|    Presidio Heights| 4.594131472394366|
|        Inner Sunset| 4.438095199935065|
|      Inner Richmond| 4.364728682713178|
|Financial Distric...| 4.344084618290155|
|      Haight Ashbury| 4.266428599285713|
|            Seacliff| 4.261111146666667|
|  West of Twin Peaks| 4.190952390857144|
|        Potrero Hill|  4.19055555742857|
|     Pacific Heights| 4.180453718900521|
|          Tenderloin|  4.10151951659727|
|Oceanview/Merced/...| 3.947242180719425|
|           Excelsior|3.9363993797169807|
|         North Beach|3.8892496403463204|
|           Lakeshore| 3.881551365094342|
|         Mission Bay|3.8548689521910124|
+--------------------+------------------+
only showing top 20 rows
    
    '''

    #q5 -Which week in the year in 2018 had the most fire calls?
    fire_cal = firecall_df.filter((col("CallDate").like("%2018%")) & (col("CallDate").isNotNull()))
    fire_cal = fire_cal.withColumn("CallDate", to_date("CallDate"))
    fire_2018 = firecall_2018.withColumn("WeekOfYear", weekofyear("CallDate"))
    df_q5 = fire_2018.groupBy(weekofyear('CallDate').alias('Week')).count().orderBy('count', ascending=False)
    df_q5.show(10,truncate = False)

    '''
    +----+-----+
|Week|count|
+----+-----+
|22  |259  |
|40  |255  |
|43  |250  |
|25  |249  |
|1   |246  |
|44  |244  |
|13  |243  |
|32  |243  |
|11  |240  |
|18  |236  |
+----+-----+
only showing top 10 rows

    '''

    #q6 - Is there a correlation between neighborhood, zip code, and number of fire calls?
    neighborhood_zipcode_counts = firecall_df.groupBy("Neighborhood", "Zipcode").agg(count("*").alias("FireCallCount"))
    q6_df = neighborhood_zipcode_counts.orderBy("Neighborhood", "Zipcode")
    q6_df.show(10,truncate = False)

    '''
    +---------------------+-------+-------------+
|Neighborhood         |Zipcode|FireCallCount|
+---------------------+-------+-------------+
|Bayview Hunters Point|94107  |179          |
|Bayview Hunters Point|94124  |9150         |
|Bayview Hunters Point|94134  |287          |
|Bernal Heights       |94110  |3109         |
|Bernal Heights       |94112  |102          |
|Bernal Heights       |94124  |49           |
|Castro/Upper Market  |94103  |51           |
|Castro/Upper Market  |94110  |61           |
|Castro/Upper Market  |94114  |3946         |
|Castro/Upper Market  |94117  |116          |
+---------------------+-------+-------------+
only showing top 10 rows
    '''

    #q7 - How can we use Parquet files or SQL tables to store this data and read it back?
    
    # #fire_df.write.parquet("fire_data.parquet")
    # used this command  to create a parquet file save data in parquet file

    # Read Parquet files back into DataFrame
    fire_df_parquet = spark.read.parquet("fire_data.parquet")
    fire_df_parquet.show(5)

    '''
+----------+------+--------------+----------------+----------+----------+--------------------+--------------------+--------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+--------------------+--------------------+-------------+---------+
|CallNumber|UnitID|IncidentNumber|        CallType|  CallDate| WatchDate|CallFinalDisposition|       AvailableDtTm|             Address|City|Zipcode|Battalion|StationArea| Box|OriginalPriority|Priority|FinalPriority|ALSUnit|CallTypeGroup|NumAlarms|UnitType|UnitSequenceInCallDispatch|FirePreventionDistrict|SupervisorDistrict|        Neighborhood|            Location|        RowID|    Delay|
+----------+------+--------------+----------------+----------+----------+--------------------+--------------------+--------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+--------------------+--------------------+-------------+---------+
+----------+------+--------------+----------------+----------+----------+--------------------+--------------------+--------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+--------------------+--------------------+-------------+---------+
|  20110016|   T13|       2003235|  Structure Fire|01/11/2002|01/10/2002|               Other|01/11/2002 01:51:...|2000 Block of CAL...|  SF|  94109|      B04|         38|3362|               3|       3|            3|  false|         NULL|        1|   TRUCK|                         2|                     4|                 5|     Pacific Heights|(37.7895840679362...|020110016-T13|     2.95|
|  20110022|   M17|       2003241|Medical Incident|01/11/2002|01/10/2002|               Other|01/11/2002 03:01:...|0 Block of SILVER...|  SF|  94124|      B10|         42|6495|               3|       3|            3|   true|         NULL|        1|   MEDIC|                         1|                    10|                10|Bayview Hunters P...|(37.7337623673897...|020110022-M17|      4.7|
|  20110023|   M41|       2003242|Medical Incident|01/11/2002|01/10/2002|               Other|01/11/2002 02:39:...|MARKET ST/MCALLIS...|  SF|  94102|      B03|         01|1455|               3|       3|            3|   true|         NULL|        1|   MEDIC|                         2|                     3|                 6|          Tenderloin|(37.7811772186856...|020110023-M41|2.4333334|
|  20110032|   E11|       2003250|    Vehicle Fire|01/11/2002|01/10/2002|               Other|01/11/2002 04:16:...|APPLETON AV/MISSI...|  SF|  94110|      B06|         32|5626|               3|       3|            3|  false|         NULL|        1|  ENGINE|                         1|                     6|                 9|      Bernal Heights|(37.7388432849018...|020110032-E11|      1.5|
|  20110043|   B04|       2003259|          Alarms|01/11/2002|01/10/2002|               Other|01/11/2002 06:01:...|1400 Block of SUT...|  SF|  94109|      B04|         03|3223|               3|       3|            3|  false|         NULL|        1|   CHIEF|                         2|                     4|                 2|    Western Addition|(37.7872890372638...|020110043-B04|3.4833333|
+----------+------+--------------+----------------+----------+----------+--------------------+--------------------+--------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+--------------------+--------------------+-------------+---------+
    '''
    

spark.stop()