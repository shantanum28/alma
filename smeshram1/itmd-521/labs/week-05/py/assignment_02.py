import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "_main_":
    if len(sys.argv) <= 0:
        sys.exit(1)

    spark = (SparkSession.builder.appName("sv-fire-calls").getOrCreate())
    #source file
    Sv_fire_calls_file = sys.argv[1]

    #Define Schema Programmatically
    print("Define Schema Programmatically")
    Sv_fire_calls_schema = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),      
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('Zipcode', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)])

    Sv_fire_calls_df = spark.read.csv(Sv_fire_calls_file, header=True, schema=Sv_fire_calls_schema)

    Sv_fire_calls_df.cache()
    Sv_fire_calls_df.printSchema()

    fire_calls_df = Sv_fire_calls_df.withColumn("CallDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))

    #1. What were all the different types of fire calls in 2018?
    print("Printing initial query")

    query1 = (fire_calls_df.select("CallType").where(year("CallDate")==2018)).limit(15)
    query1.show()
    """ Output: 
+-----------------+
|         CallType|
+-----------------+
|   Structure Fire|
|           HazMat|
|           Alarms|
| Medical Incident|
| Medical Incident|
|Electrical Hazard|
| Medical Incident|
| Medical Incident|
| Medical Incident|
| Medical Incident|
| Medical Incident|
| Medical Incident|
|           Alarms|
|           Alarms|
|           Alarms|
+-----------------+
    only showing top 15 rows"""

    #2. What months within the year 2018 saw the highest number of fire calls?
    print("Displaying the Second query")

    query2 = fire_calls_df.filter(year("CallDate") == 2018).groupBy(month("CallDate").alias("CallMonth")).agg(count("CallNumber").alias("CountOfCallNumber")).orderBy("CountOfCallNumber",ascending=False).limit(5)
    query2.show()
    """ Output:
+---------+-----------------+
|CallMonth|CountOfCallNumber|
+---------+-----------------+
|       10|             1068|
|        5|             1047|
|        3|             1029|
|        8|             1021|
|        1|             1007|
+---------+-----------------+
    
    only showing top 5 rows"""

    #3. Which neighborhood in San Francisco generated the most fire calls in 2018?
    print("Displaying the Third query")

    query3 = fire_calls_df.filter((year("CallDate") == 2018)&(col("Neighborhood").isNotNull())).groupBy("Neighborhood").agg(count("Neighborhood").alias("Neighborhood_Sv_Fire_Calls")).orderBy("Neighborhood_Sv_Fire_Calls",ascending=False).limit(20)
    query3.show()
    """ Output:
+--------------------+--------------------------+
|        Neighborhood|Neighborhood_Sv_Fire_Calls|
+--------------------+--------------------------+
|          Tenderloin|                      1393|
|     South of Market|                      1053|
|             Mission|                       913|
|Financial Distric...|                       772|
|Bayview Hunters P...|                       522|
|    Western Addition|                       352|
|     Sunset/Parkside|                       346|
|            Nob Hill|                       295|
|        Hayes Valley|                       291|
|      Outer Richmond|                       262|
| Castro/Upper Market|                       251|
|         North Beach|                       231|
|           Excelsior|                       212|
|        Potrero Hill|                       210|
|  West of Twin Peaks|                       210|
|           Chinatown|                       191|
|              Marina|                       191|
|     Pacific Heights|                       191|
|         Mission Bay|                       178|
|      Bernal Heights|                       170|
+--------------------+--------------------------+
    only showing top 20 rows"""

    #3.1 Generated most fire calls
    query3.limit(1).show()
    """ Output:
    +------------+--------------------------+
    |Neighborhood|Neighborhood_SF_Fire_Calls|
    +------------+--------------------------+
    |  Tenderloin|                      1393|
    +------------+--------------------------+
    """

    #4. Which neighborhoods had the worst response times to fire calls in 2018?
    print("Displaying the Fourth query")

    query4 = fire_calls_df.filter((year("CallDate") == 2018)&(col("Neighborhood").isNotNull())).groupBy("Neighborhood").agg(avg(col("Delay")).alias("avg_delay")).orderBy("avg_delay", ascending=True).limit(15)
    query4.show()
    """ Output:
+-----------------+------------------+
|     Neighborhood|         avg_delay|
+-----------------+------------------+
|Lone Mountain/USF|3.2472222397724786|
|        Glen Park| 3.277777776122093|
| Western Addition|3.2843276457861066|
|     Lincoln Park|3.3111111190583973|
|          Portola|3.3385542175137854|
|        Japantown| 3.347695054526025|
|             None| 3.363333320617676|
|     Hayes Valley|3.3703894590082037|
|           Marina|3.4267888378098372|
|          Mission|3.4505293849459875|
|Visitacion Valley|3.4838095208009086|
|       Noe Valley| 3.525949378556843|
|  Sunset/Parkside|3.5842003932147355|
|    Outer Mission|   3.6317518444827|
|   Outer Richmond|3.6480915955346047|
+-----------------+------------------+
    only showing top 15 rows"""

    #4.1 worst response times to fire calls in 2018
    query4.limit(1).show()
    """ Output:
    +-----------------+------------------+
    |     Neighborhood|         avg_delay|
    +-----------------+------------------+
    |Lone Mountain/USF|3.2472222397724786|
    +-----------------+------------------+
    """

    #5. Which week in the year in 2018 had the most fire calls?
    print("Displaying the Fifth query")

    query5 = fire_calls_df.filter(year("CallDate") == 2018).groupBy(weekofyear("CallDate").alias("Week_Of_the_Year")).agg(count("CallNumber").alias("Count_of_CallNumber")).orderBy("Count_Of_CallNumber",ascending=False).limit(15)
    query5.show()
    """ Output:
+----------------+-------------------+
|Week_Of_the_Year|Count_of_CallNumber|
+----------------+-------------------+
|              22|                259|
|              40|                255|
|              43|                250|
|              25|                249|
|               1|                246|
|              44|                244|
|              13|                243|
|              32|                243|
|              11|                240|
|               5|                236|
|              18|                236|
|              23|                235|
|              31|                234|
|              42|                234|
|               2|                234|
+----------------+-------------------+
    only showing top 15 rows"""

    #5.1 week in the year in 2018 had the most fire calls
    query5.limit(1).show()
    """ Output:
    +-------------+-----------------+
    |WeekOfTheYear|CountOfCallNumber|
    +-------------+-----------------+
    |           22|              259|
    +-------------+-----------------+
    """

    #6. Is there a correlation between neighborhood, zip code, and number of fire calls?
    print("Displaying the Sixth query")

    query6 = fire_calls_df.filter(col("Neighborhood").isNotNull()).groupBy("Neighborhood", "Zipcode").count().withColumnRenamed("count", "NumberOfCalls").orderBy("NumberOfCalls").limit(15)
    query6.show()
    """ Output:
+--------------------+-------+-------------+
|        Neighborhood|Zipcode|NumberOfCalls|
+--------------------+-------+-------------+
|     South of Market|  94105|            1|
|           Excelsior|   NULL|            1|
|            Presidio|  94118|            2|
| Castro/Upper Market|  94131|            2|
|            Presidio|  94115|            2|
|          Twin Peaks|  94127|            3|
|    Presidio Heights|  94129|            3|
|        Potrero Hill|  94103|            5|
|                None|  94124|            7|
|                None|  94158|           11|
|Oceanview/Merced/...|  94127|           12|
|             Portola|  94124|           14|
|        Potrero Hill|  94124|           16|
|      Outer Richmond|  94122|           17|
|        Inner Sunset|  94114|           20|
+--------------------+-------+-------------+
    only showing top 15 rows"""

    #7. How can we use Parquet files or SQL tables to store this data and read it back?
    print("Displaying the Seventh query")

    query7 = fire_calls_df.write.parquet("fire_ts_df.parquet", mode="overwrite")
    parquet_schema = spark.read.parquet("fire_ts_df.parquet")
    parquet_schema.printSchema()

    """ Output:

    24/02/22 19:53:33 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "CallNumber",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "UnitID",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "IncidentNumber",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallType",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallDate",
    "type" : "timestamp",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "WatchDate",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallFinalDisposition",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "AvailableDtTm",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Address",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "City",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Zipcode",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Battalion",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "StationArea",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Box",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "OriginalPriority",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Priority",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "FinalPriority",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "ALSUnit",
    "type" : "boolean",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallTypeGroup",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "NumAlarms",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "UnitType",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "UnitSequenceInCallDispatch",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "FirePreventionDistrict",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "SupervisorDistrict",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Neighborhood",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Location",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "RowID",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Delay",
    "type" : "float",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional int32 CallNumber;
  optional binary UnitID (STRING);
  optional int32 IncidentNumber;
  optional binary CallType (STRING);
  optional int96 CallDate;
  optional binary WatchDate (STRING);
  optional binary CallFinalDisposition (STRING);
  optional binary AvailableDtTm (STRING);
  optional binary Address (STRING);
  optional binary City (STRING);
  optional int32 Zipcode;
  optional binary Battalion (STRING);
  optional binary StationArea (STRING);
  optional binary Box (STRING);
  optional binary OriginalPriority (STRING);
  optional binary Priority (STRING);
  optional int32 FinalPriority;
  optional boolean ALSUnit;
  optional binary CallTypeGroup (STRING);
  optional int32 NumAlarms;
  optional binary UnitType (STRING);
  optional int32 UnitSequenceInCallDispatch;
  optional binary FirePreventionDistrict (STRING);
  optional binary SupervisorDistrict (STRING);
  optional binary Neighborhood (STRING);
  optional binary Location (STRING);
  optional binary RowID (STRING);
  optional float Delay;
}


24/02/22 19:53:33 INFO ParquetWriteSupport: Initialized Parquet WriteSupport with Catalyst schema:
{
  "type" : "struct",
  "fields" : [ {
    "name" : "CallNumber",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "UnitID",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "IncidentNumber",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallType",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallDate",
    "type" : "timestamp",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "WatchDate",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallFinalDisposition",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "AvailableDtTm",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Address",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "City",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Zipcode",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Battalion",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "StationArea",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Box",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "OriginalPriority",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Priority",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "FinalPriority",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "ALSUnit",
    "type" : "boolean",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "CallTypeGroup",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "NumAlarms",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "UnitType",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "UnitSequenceInCallDispatch",
    "type" : "integer",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "FirePreventionDistrict",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "SupervisorDistrict",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Neighborhood",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Location",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "RowID",
    "type" : "string",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "Delay",
    "type" : "float",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional int32 CallNumber;
  optional binary UnitID (STRING);
  optional int32 IncidentNumber;
  optional binary CallType (STRING);
  optional int96 CallDate;
  optional binary WatchDate (STRING);
  optional binary CallFinalDisposition (STRING);
  optional binary AvailableDtTm (STRING);
  optional binary Address (STRING);
  optional binary City (STRING);
  optional int32 Zipcode;
  optional binary Battalion (STRING);
  optional binary StationArea (STRING);
  optional binary Box (STRING);
  optional binary OriginalPriority (STRING);
  optional binary Priority (STRING);
  optional int32 FinalPriority;
  optional boolean ALSUnit;
  optional binary CallTypeGroup (STRING);
  optional int32 NumAlarms;
  optional binary UnitType (STRING);
  optional int32 UnitSequenceInCallDispatch;
  optional binary FirePreventionDistrict (STRING);
  optional binary SupervisorDistrict (STRING);
  optional binary Neighborhood (STRING);
  optional binary Location (STRING);
  optional binary RowID (STRING);
  optional float Delay;
}

    root
 |-- CallNumber: integer (nullable = true)
 |-- UnitID: string (nullable = true)
 |-- IncidentNumber: integer (nullable = true)
 |-- CallType: string (nullable = true)
 |-- CallDate: timestamp (nullable = true)
 |-- WatchDate: string (nullable = true)
 |-- CallFinalDisposition: string (nullable = true)
 |-- AvailableDtTm: string (nullable = true)
 |-- Address: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Zipcode: integer (nullable = true)
 |-- Battalion: string (nullable = true)
 |-- StationArea: string (nullable = true)
 |-- Box: string (nullable = true)
 |-- OriginalPriority: string (nullable = true)
 |-- Priority: string (nullable = true)
 |-- FinalPriority: integer (nullable = true)
 |-- ALSUnit: boolean (nullable = true)
 |-- CallTypeGroup: string (nullable = true)
 |-- NumAlarms: integer (nullable = true)
 |-- UnitType: string (nullable = true)
 |-- UnitSequenceInCallDispatch: integer (nullable = true)
 |-- FirePreventionDistrict: string (nullable = true)
 |-- SupervisorDistrict: string (nullable = true)
 |-- Neighborhood: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- RowID: string (nullable = true)
 |-- Delay: float (nullable = true)

24/02/22 19:53:39 INFO SparkContext: Invoking stop() from shutdown hook
    """