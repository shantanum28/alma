from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, to_date, date_format
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit assignment_02.py <file_path>", file=sys.stderr)
        sys.exit(-1)

    # Create a Spark session
    spark = SparkSession.builder.appName("FireCallsAnalysis").getOrCreate()

    # Set the configuration to display complete table
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)

    # Define the schema
    fire_schema = StructType([
        StructField('CallNumber', IntegerType(), True),
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

    # Load the dataset into a DataFrame
    fire_calls_df = spark.read.csv(sys.argv[1], header=True, schema=fire_schema)

    # Convert 'CallDate' to DateType
    fire_calls_df = fire_calls_df.withColumn('CallDate', to_date('CallDate', 'MM/dd/yyyy'))

    # Add a new column 'WeekOfYear'
    fire_calls_df = fire_calls_df.withColumn('WeekOfYear', weekofyear('CallDate'))

    # Question 1: What were all the different types of fire calls in 2018?
    # Answer:
    print("Question 1:")
    fire_calls_df.filter(col('CallDate').contains('2018')).select('CallType').distinct().show(truncate=False)

    # +-------------------------------+
    # |CallType                       |
    # +-------------------------------+
    # |Elevator / Escalator Rescue    |
    # |Alarms                         |
    # |Odor (Strange / Unknown)       |
    # |Citizen Assist / Service Call  |
    # |HazMat                         |
    # |Vehicle Fire                   |
    # |Other                          |
    # |Outside Fire                   |
    # |Traffic Collision              |
    # |Assist Police                  |
    # |Gas Leak (Natural and LP Gases)|
    # |Water Rescue                   |
    # |Electrical Hazard              |
    # |Structure Fire                 |
    # |Medical Incident               |
    # |Fuel Spill                     |
    # |Smoke Investigation (Outside)  |
    # |Train / Rail Incident          |
    # |Explosion                      |
    # |Suspicious Package             |
    # +-------------------------------+

    # Question 2: What months within the year 2018 saw the highest number of fire calls?
    # Answer:
    print("Question 2:")
    fire_calls_df.filter(col('CallDate').contains('2018')).groupBy(date_format('CallDate', 'MMM').alias('Month')).count().sort('count', ascending=False).show(10, truncate=False)

    # +-----+-----+
    # |Month|count|
    # +-----+-----+
    # |Oct  |1068 |
    # |May  |1047 |
    # |Mar  |1029 |
    # |Aug  |1021 |
    # |Jan  |1007 |
    # |Jun  |974  |
    # |Jul  |974  |
    # |Sep  |951  |
    # |Apr  |947  |
    # |Feb  |919  |
    # +-----+-----+

    # Question 3: Which neighborhood in San Francisco generated the most fire calls in 2018?
    # Answer:
    print("Question 3:")
    fire_calls_df.filter(col('CallDate').contains('2018')).groupBy('Neighborhood').count().sort('count', ascending=False).show(1, truncate=False)

    # +------------+-----+
    # |Neighborhood|count|
    # +------------+-----+
    # |Tenderloin  |1393 |
    # +------------+-----+

    # Question 4: Which neighborhoods had the worst response times to fire calls in 2018?
    # Answer:
    print("Question 4:")
    fire_calls_df.filter(col('CallDate').contains('2018')).groupBy('Neighborhood').agg({'Delay': 'avg'}).sort('avg(Delay)', ascending=False).show(10, truncate=False)

    # +------------------------------+-----------------+
    # |Neighborhood                  |avg(Delay)       |
    # +------------------------------+-----------------+
    # |Chinatown                     |6.190314101143033|
    # |Presidio                      |5.829227011272873|
    # |Treasure Island               |5.453703684111436|
    # |McLaren Park                  |4.74404764175415 |
    # |Bayview Hunters Point         |4.620561962212182|
    # |Presidio Heights              |4.594131482319093|
    # |Inner Sunset                  |4.438095217981896|
    # |Inner Richmond                |4.364728709292966|
    # |Financial District/South Beach|4.344084616885593|
    # |Haight Ashbury                |4.266428579390049|
    # +------------------------------+-----------------+

    # Question 5: Which week in the year in 2018 had the most fire calls?
    # Answer:
    print("Question 5:")
    fire_calls_df.filter(col('CallDate').contains('2018')).groupBy('WeekOfYear').count().sort('count', ascending=False).show(1, truncate=False)

    # +----------+-----+
    # |WeekOfYear|count|
    # +----------+-----+
    # |22        |259  |
    # +----------+-----+

    # Question 6: Is there a correlation between neighborhood, zip code, and the number of fire calls?
    # Answer:
    print("Question 6:")
    correlation_df = fire_calls_df.groupBy('Neighborhood', 'Zipcode').agg({'NumAlarms': 'sum'})
    correlation_df.show(10, truncate=False)

    # +--------------------------+-------+--------------+
    # |Neighborhood              |Zipcode|sum(NumAlarms)|
    # +--------------------------+-------+--------------+
    # |Inner Sunset              |94122  |2183          |
    # |Bayview Hunters Point     |94124  |9238          |
    # |Inner Sunset              |94114  |20            |
    # |West of Twin Peaks        |94112  |763           |
    # |Haight Ashbury            |94114  |21            |
    # |Glen Park                 |94110  |25            |
    # |Excelsior                 |94112  |3248          |
    # |Russian Hill              |94109  |2279          |
    # |None                      |94124  |7             |
    # |Chinatown                 |94133  |1874          |
    # +--------------------------+-------+--------------+

    # Question 7: How can we use Parquet files or SQL tables to store this data and read it back?
    # Answer:
    print("Question 7:")

    # Writing DataFrame to a Parquet file
    fire_calls_df.write.parquet("parquet_output")

    # Reading the Parquet file back to a DataFrame
    parquet_df = spark.read.parquet("parquet_output")
    parquet_df.limit(10).show(truncate=False)

    # +----------+------+--------------+----------------+----------+----------+--------------------+----------------------+------------------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------------+--------------------------+----------------------+------------------+------------------------------+-------------------------------------+-------------+---------+----------+
    # |CallNumber|UnitID|IncidentNumber|CallType        |CallDate  |WatchDate |CallFinalDisposition|AvailableDtTm         |Address                       |City|Zipcode|Battalion|StationArea|Box |OriginalPriority|Priority|FinalPriority|ALSUnit|CallTypeGroup|NumAlarms|UnitType      |UnitSequenceInCallDispatch|FirePreventionDistrict|SupervisorDistrict|Neighborhood                  |Location                             |RowID        |Delay    |WeekOfYear|
    # +----------+------+--------------+----------------+----------+----------+--------------------+----------------------+------------------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------------+--------------------------+----------------------+------------------+------------------------------+-------------------------------------+-------------+---------+----------+
    # |20110016  |T13   |2003235       |Structure Fire  |2002-01-11|01/10/2002|Other               |01/11/2002 01:51:44 AM|2000 Block of CALIFORNIA ST   |SF  |94109  |B04      |38         |3362|3               |3       |3            |false  |NULL         |1        |TRUCK         |2                         |4                     |5                 |Pacific Heights               |(37.7895840679362, -122.428071912459)|020110016-T13|2.95     |2         |
    # |20110022  |M17   |2003241       |Medical Incident|2002-01-11|01/10/2002|Other               |01/11/2002 03:01:18 AM|0 Block of SILVERVIEW DR      |SF  |94124  |B10      |42         |6495|3               |3       |3            |true   |NULL         |1        |MEDIC         |1                         |10                    |10                |Bayview Hunters Point         |(37.7337623673897, -122.396113802632)|020110022-M17|4.7      |2         |
    # |20110023  |M41   |2003242       |Medical Incident|2002-01-11|01/10/2002|Other               |01/11/2002 02:39:50 AM|MARKET ST/MCALLISTER ST       |SF  |94102  |B03      |01         |1455|3               |3       |3            |true   |NULL         |1        |MEDIC         |2                         |3                     |6                 |Tenderloin                    |(37.7811772186856, -122.411699931232)|020110023-M41|2.4333334|2         |
    # |20110032  |E11   |2003250       |Vehicle Fire    |2002-01-11|01/10/2002|Other               |01/11/2002 04:16:46 AM|APPLETON AV/MISSION ST        |SF  |94110  |B06      |32         |5626|3               |3       |3            |false  |NULL         |1        |ENGINE        |1                         |6                     |9                 |Bernal Heights                |(37.7388432849018, -122.423948785199)|020110032-E11|1.5      |2         |
    # |20110043  |B04   |2003259       |Alarms          |2002-01-11|01/10/2002|Other               |01/11/2002 06:01:58 AM|1400 Block of SUTTER ST       |SF  |94109  |B04      |03         |3223|3               |3       |3            |false  |NULL         |1        |CHIEF         |2                         |4                     |2                 |Western Addition              |(37.7872890372638, -122.424236212664)|020110043-B04|3.4833333|2         |
    # |20110072  |T08   |2003279       |Structure Fire  |2002-01-11|01/11/2002|Other               |01/11/2002 08:03:26 AM|BEALE ST/FOLSOM ST            |SF  |94105  |B03      |35         |2122|3               |3       |3            |false  |NULL         |1        |TRUCK         |2                         |3                     |6                 |Financial District/South Beach|(37.7886866619654, -122.392722833778)|020110072-T08|1.75     |2         |
    # |20110125  |E33   |2003301       |Alarms          |2002-01-11|01/11/2002|Other               |01/11/2002 09:46:44 AM|0 Block of FARALLONES ST      |SF  |94112  |B09      |33         |8324|3               |3       |3            |false  |NULL         |1        |ENGINE        |2                         |9                     |11                |Oceanview/Merced/Ingleside    |(37.7140353531157, -122.454117149916)|020110125-E33|2.7166667|2         |
    # |20110130  |E36   |2003304       |Alarms          |2002-01-11|01/11/2002|Other               |01/11/2002 09:58:53 AM|600 Block of POLK ST          |SF  |94102  |B02      |03         |3114|3               |3       |3            |false  |NULL         |1        |ENGINE        |1                         |2                     |6                 |Tenderloin                    |(37.7826266328595, -122.41915582123) |020110130-E36|1.7833333|2         |
    # |20110197  |E05   |2003343       |Medical Incident|2002-01-11|01/11/2002|Other               |01/11/2002 12:06:57 PM|1500 Block of WEBSTER ST      |SF  |94115  |B04      |05         |3513|3               |3       |3            |false  |NULL         |1        |ENGINE        |1                         |4                     |5                 |Japantown                     |(37.784958590666, -122.431435274503) |020110197-E05|1.5166667|2         |
    # |20110215  |E06   |2003348       |Medical Incident|2002-01-11|01/11/2002|Other               |01/11/2002 01:08:40 PM|DIAMOND ST/MARKET ST          |SF  |94114  |B05      |06         |5415|3               |3       |3            |false  |NULL         |1        |ENGINE        |1                         |5                     |8                 |Castro/Upper Market           |(37.7618954753708, -122.437298717721)|020110215-E06|2.7666667|2         |
    # +----------+------+--------------+----------------+----------+----------+--------------------+----------------------+------------------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------------+--------------------------+----------------------+------------------+------------------------------+-------------------------------------+-------------+---------+----------+

    # Alternatively, we can create a temporary SQL table
    fire_calls_df.createOrReplaceTempView("fire_calls_table")

    # Querying the SQL table
    sql_result = spark.sql("SELECT * FROM fire_calls_table WHERE CallDate LIKE '2018%'")
    sql_result.limit(10).show(truncate=False)

    # +----------+------+--------------+-----------------+----------+----------+--------------------------+----------------------+---------------------------------+-------------+-------+---------+-----------+----+----------------+--------+-------------+-------+----------------------------+---------+--------+--------------------------+----------------------+------------------+------------------------------+-----------------------------------------+---------------+---------+----------+
    # |CallNumber|UnitID|IncidentNumber|CallType         |CallDate  |WatchDate |CallFinalDisposition      |AvailableDtTm         |Address                          |City         |Zipcode|Battalion|StationArea|Box |OriginalPriority|Priority|FinalPriority|ALSUnit|CallTypeGroup               |NumAlarms|UnitType|UnitSequenceInCallDispatch|FirePreventionDistrict|SupervisorDistrict|Neighborhood                  |Location                                 |RowID          |Delay    |WeekOfYear|
    # +----------+------+--------------+-----------------+----------+----------+--------------------------+----------------------+---------------------------------+-------------+-------+---------+-----------+----+----------------+--------+-------------+-------+----------------------------+---------+--------+--------------------------+----------------------+------------------+------------------------------+-----------------------------------------+---------------+---------+----------+
    # |180190613 |E10   |18007977      |Structure Fire   |2018-01-19|01/18/2018|Fire                      |01/19/2018 07:40:18 AM|3200 Block of CLAY ST            |San Francisco|94115  |B04      |10         |4335|3               |3       |3            |true   |Alarm                       |1        |ENGINE  |1                         |4                     |2                 |Presidio Heights              |(37.789214135744714, -122.44646398918253)|180190613-E10  |2.8833334|3         |
    # |180190640 |T08   |18007978      |HazMat           |2018-01-19|01/18/2018|Fire                      |01/19/2018 07:58:57 AM|200 Block of KING ST             |San Francisco|94107  |B03      |08         |2171|3               |3       |3            |false  |Alarm                       |1        |TRUCK   |2                         |3                     |6                 |Mission Bay                   |(37.77732776352611, -122.39308855968541) |180190640-T08  |6.3333335|3         |
    # |180190750 |T02   |18007983      |Alarms           |2018-01-19|01/19/2018|Fire                      |01/19/2018 08:33:14 AM|900 Block of CLAY ST             |San Francisco|94108  |B01      |02         |1355|3               |3       |3            |false  |Alarm                       |1        |TRUCK   |2                         |1                     |3                 |Chinatown                     |(37.79406255571054, -122.40832278818522) |180190750-T02  |2.65     |3         |
    # |180190755 |E08   |18007984      |Medical Incident |2018-01-19|01/19/2018|Code 2 Transport          |01/19/2018 08:45:01 AM|600 Block of 2ND ST              |San Francisco|94107  |B03      |08         |2156|3               |3       |3            |true   |Potentially Life-Threatening|1        |ENGINE  |2                         |3                     |6                 |Financial District/South Beach|(37.78118776896415, -122.3913514497218)  |180190755-E08  |3.5333333|3         |
    # |180190868 |65    |18007989      |Medical Incident |2018-01-19|01/19/2018|Code 2 Transport          |01/19/2018 09:48:53 AM|100 Block of GOLDEN GATE AVE     |San Francisco|94102  |B02      |03         |1546|3               |3       |3            |true   |Potentially Life-Threatening|1        |MEDIC   |1                         |2                     |6                 |Tenderloin                    |(37.78202243717766, -122.4130541482534)  |180190868-65   |1.1      |3         |
    # |180190966 |E17   |18007998      |Electrical Hazard|2018-01-19|01/19/2018|Fire                      |01/19/2018 09:47:51 AM|2600 Block of ARELIOUS WALKER DR |San Francisco|94124  |B10      |17         |6615|3               |3       |3            |true   |Alarm                       |1        |ENGINE  |1                         |10                    |10                |Bayview Hunters Point         |(37.719059825602756, -122.38465573055352)|180190966-E17  |4.05     |3         |
    # |180191014 |60    |18008005      |Medical Incident |2018-01-19|01/19/2018|Code 2 Transport          |01/19/2018 10:48:32 AM|400 Block of 8TH AVE             |San Francisco|94118  |B07      |31         |7135|3               |3       |3            |true   |Potentially Life-Threatening|1        |MEDIC   |1                         |7                     |1                 |Inner Richmond                |(37.779962389810564, -122.46628840843132)|180191014-60   |2.5666666|3         |
    # |180191025 |E22   |18008006      |Medical Incident |2018-01-19|01/19/2018|Code 2 Transport          |01/19/2018 10:11:37 AM|0 Block of LOCKSLEY AVE          |San Francisco|94122  |B08      |22         |7331|A               |3       |3            |false  |Potentially Life-Threatening|1        |ENGINE  |1                         |8                     |7                 |Inner Sunset                  |(37.75922029758928, -122.46331160714958) |180191025-E22  |1.4      |3         |
    # |180191138 |88    |18008017      |Medical Incident |2018-01-19|01/19/2018|No Merit                  |01/19/2018 10:33:38 AM|1800 Block of IRVING ST          |San Francisco|94122  |B08      |22         |7424|3               |3       |3            |true   |Potentially Life-Threatening|1        |MEDIC   |2                         |8                     |4                 |Sunset/Parkside               |(37.763591973361024, -122.47768629741721)|180191138-88   |2.6666667|3         |
    # |180191308 |AM108 |18008031      |Medical Incident |2018-01-19|01/19/2018|Patient Declined Transport|01/19/2018 11:08:25 AM|100 Block of 6TH ST              |San Francisco|94103  |B03      |01         |2251|3               |3       |3            |false  |Potentially Life-Threatening|1        |PRIVATE |3                         |3                     |6                 |South of Market               |(37.78079208027532, -122.40838574549893) |180191308-AM108|1.7666667|3         |
    # +----------+------+--------------+-----------------+----------+----------+--------------------------+----------------------+---------------------------------+-------------+-------+---------+-----------+----+----------------+--------+-------------+-------+----------------------------+---------+--------+--------------------------+----------------------+------------------+------------------------------+-----------------------------------------+---------------+---------+----------+

    # Stop the Spark session
    spark.stop()
