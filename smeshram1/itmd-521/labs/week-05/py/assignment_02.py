from os import truncate
import sys

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import *
from pyspark.sql.functions import col, count, desc, month, to_date, year, avg, weekofyear, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: DivvyTrips <file>", file=sys.stderr)
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
    
    fire_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data_source)

#Question-1 - What were all the different types of fire calls in 2018?
    fire_df = fire_df.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
    calls_2018 = fire_df.filter(year(col("CallDate")) == 2018)
    distinct_call_types_2018 = calls_2018.select("CallType").distinct()
    distinct_call_types_2018.show(truncate=False)
#Output
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
#Question-2 - What months within the year 2018 saw the highest number of fire calls?

    fire_2018 = fire_df.filter(col("CallDate").like("%2018%") & col("CallDate").isNotNull())
    fire_2018 = fire_2018.withColumn("CallDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    fire_2018 = fire_2018.withColumn("Month", month("CallDate"))
    df_q2 = fire_2018.groupBy("Month").count().orderBy(desc("count"))

    df_q2.show(12,truncate = False)
#Output
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
   
#Question-3 - Which neighborhood in San Francisco generated the most fire calls in 2018?
nhood_calls = fire_2018.groupBy("Neighborhood").agg(count("*").alias("TotalCalls")).orderBy(desc("TotalCalls"))
nhood_calls.show(3)
#Output
'''
+---------------+----------+
|   Neighborhood|TotalCalls|
+---------------+----------+
|     Tenderloin|      1393|
|South of Market|      1053|
|        Mission|       913|
+---------------+----------+
''' 
#Question-4 - Which neighborhoods had the worst response times to fire calls in 2018?
fire_call_2018 = fire_df.filter((col("CallDate").like("%2018%"))& (col("CallDate").isNotNull()))
    
nhood_response = fire_call_2018.groupBy("Neighborhood").agg(avg("Delay").alias("AverageDelay"))
    
worst_response = nhood_response.orderBy(desc("AverageDelay"))
print("Worst Response Times to Fire Calls in 2018:")
worst_response.show()
#Output
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
'''
#Question-5 -Which week in the year in 2018 had the most fire calls?
fire_cal = fire_df.filter((col("CallDate").like("%2018%")) & (col("CallDate").isNotNull()))
fire_cal = fire_cal.withColumn("CallDate", to_date("CallDate"))
    
weekly_call_counts_2018 = fire_cal.groupBy(weekofyear('CallDate').alias('Week')).count().orderBy('count', ascending=False)
    
print("Week in the year 2018 with the most fire calls:")
weekly_call_counts_2018.show(1)
#Output
'''
+----+-----+
|Week|count|
+----+-----+
|  22|  259|
+----+-----+
'''
#Question-6 - Is there a correlation between neighborhood, zip code, and number of fire calls?
neighborhood_zipcode_counts = fire_df.groupBy("Neighborhood", "Zipcode").agg(count("*").alias("FireCallCount"))
neighborhood_zipcode_counts.show()
#Output
'''
+--------------------+-------+-------------+
|        Neighborhood|Zipcode|FireCallCount|
+--------------------+-------+-------------+
|        Inner Sunset|  94122|         2161|
|Bayview Hunters P...|  94124|         9150|
|        Inner Sunset|  94114|           20|
|  West of Twin Peaks|  94112|          760|
|      Haight Ashbury|  94114|           21|
|           Glen Park|  94110|           25|
|           Excelsior|  94112|         3237|
|        Russian Hill|  94109|         2261|
|                None|  94124|            7|
|           Chinatown|  94133|         1861|
|     Pacific Heights|  94115|         2100|
|Oceanview/Merced/...|  94127|           12|
|        Potrero Hill|  94103|            5|
|        Inner Sunset|  94117|          224|
|    Golden Gate Park|  94117|          107|
|                None|   NULL|          141|
|          Noe Valley|  94131|          763|
|    Western Addition|  94117|          315|
|            Presidio|  94118|            2|
|        McLaren Park|  94112|           36|
+--------------------+-------+-------------+
'''
#Question-7
fire_calls_2018.write.mode('overwrite').parquet("fire_call_types_2018.parquet")
parquet_data = spark.read.parquet("fire_call_types_2018.parquet")
parquet_data.show(5)
#Output

spark.stop()