import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql.functions import unix_timestamp, avg, col, to_timestamp, to_date
from pyspark.sql.functions import year, to_date, month, when, weekofyear, desc
import pyspark.sql.functions as F
from pyspark.sql.functions import to_date
from pyspark.sql.types import DateType
from pyspark.sql.functions import corr


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: sf-fire-calls <file>", file=sys.stderr)
        sys.exit(-1)

    # Initialize SparkSession
    spark = SparkSession.builder.appName("SF Fire Calls Analysis").getOrCreate()
    
    # Programmatic way to define a schema
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

    # Use the DataFrameReader interface to read a CSV file
    sf_fire_file = sys.argv[1]

    fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
    
    # Print the programmatically attached schema
    print("\nProgrammatically Attached Schema:")
    fire_df.printSchema()

    #Convert "CallDate" column to Date type for Year
    fire_df = fire_df.withColumn("Year", year(to_date("CallDate", "MM/dd/yyyy")))


    ##############

    ### Question 1: What were all the different types of fire calls in 2018?

    Filter DataFrame for incidents in 2018
    fire_df_2018 = fire_df.filter(col("CallDate").contains("2018"))

    Count occurrences of each unique call type
    call_types_2018 = (fire_df_2018.select("CallType").where(col("CallType").isNotNull()).groupBy("CallType").count().orderBy("count", ascending=False))

    Show the result
    print("\nDifferent types of fire calls in 2018:")
    call_types_2018.show(10, truncate=False)

    '''
    +-------------------------------+-----+
    |CallType                       |count|
    +-------------------------------+-----+
    |Medical Incident               |7004 |
    |Alarms                         |1144 |
    |Structure Fire                 |906  |
    |Traffic Collision              |433  |
    |Outside Fire                   |153  |
    |Other                          |114  |
    |Citizen Assist / Service Call  |113  |
    |Gas Leak (Natural and LP Gases)|69   |
    |Water Rescue                   |43   |
    |Elevator / Escalator Rescue    |36   |
    +-------------------------------+-----+
    only showing top 10 rows

    '''
    ### Question 2: What months within the year 2018 saw the highest number of fire calls?

    Convert "CallDate" column to Date type for CallDate
    fire_df = fire_df.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))

    # Group by month and count occurrences, then find the month with the highest count
    #highest_calls_df = (fire_df.filter(year(to_date("CallDate", "MM/dd/yyyy")) == 2018).groupBy(month("CallDate").alias("Month")).count().orderBy("count", ascending=False))
    
    #highest_calls_df.show()

    # Get the month with the highest count
    #highest_fire_calls_month = highest_calls_df.select("Month").first()

    # Show the result
    #print("\nMonth within the year 2018 with the highest number of fire calls:", highest_fire_calls_month)

    '''
    +-----+-----+
    |Month|count|
    +-----+-----+
    |   10| 1068|
    |    5| 1047|
    |    3| 1029|
    |    8| 1021|
    |    1| 1007|
    |    6|  974|
    |    7|  974|
    |    9|  951|
    |    4|  947|
    |    2|  919|
    |   11|  199|
    +-----+-----+
    Month within the year 2018 with the highest number of fire calls: Row(Month=10)
    
    '''

    ### Question 3: Which neighborhood in San Francisco generated the most fire calls in 2018?

    #Group by neighborhood and count occurrences, then find the neighborhood with the highest count
    #neighborhood_counts = (fire_df.filter(fire_df.Year==2018).groupBy("Neighborhood").count().orderBy(col("count").desc()))
    
    #print("\nTop 10 Neighborhoods in San Francisco with most fire calls in 2018 (in descending order of count):")
    #neighborhood_counts.show(10, truncate=False)
    
    #Show the result for most fire calls neighbourhood
    #most_calls_neighborhood = neighborhood_counts.first()["Neighborhood"]
    #print("\nNeighborhood in San Francisco with the most fire calls in 2018:", most_calls_neighborhood)

    '''
    Top 10 Neighborhoods in San Francisco with most fire calls in 2018 (in descending order of count):
    +------------------------------+-----+
    |Neighborhood                  |count|
    +------------------------------+-----+
    |Tenderloin                    |1393 |
    |South of Market               |1053 |
    |Mission                       |913  |
    |Financial District/South Beach|772  |
    |Bayview Hunters Point         |522  |
    |Western Addition              |352  |
    |Sunset/Parkside               |346  |
    |Nob Hill                      |295  |
    |Hayes Valley                  |291  |
    |Outer Richmond                |262  |
    +------------------------------+-----+
    only showing top 10 rows

    Neighborhood in San Francisco with the most fire calls in 2018: Tenderloin

    '''
    ### Question 4: Which neighborhoods had the worst response times to fire calls in 2018?

    #Calculating response time for each neighbourhood and printing top 10 rows 
    #response_time_neighbourhood_2018 = (fire_df.filter(fire_df.Year == 2018).groupBy("Neighborhood").agg(avg("Delay").alias("AvgResponseTime")).orderBy("AvgResponseTime", ascending=False))

    #response_time_neighbourhood_2018.show(10, truncate=False)

    #Show the result for worst response time to fire calls
    #worst_response_time_neighbourhood_2018 = response_time_neighbourhood_2018.first()["Neighborhood"]
    #print("\nNeighborhood in San Francisco with the worst response time to fire calls in 2018:", worst_response_time_neighbourhood_2018)

    '''
    +------------------------------+-----------------+
    |Neighborhood                  |AvgResponseTime  |
    +------------------------------+-----------------+
    |Chinatown                     |6.190314101143033|
    |Presidio                      |5.829227011272873|
    |Treasure Island               |5.453703684111436|
    |McLaren Park                  |4.74404764175415 |
    |Bayview Hunters Point         |4.620561962212182|
    |Presidio Heights              |4.594131482319093|
    |Inner Sunset                  |4.438095217981896|
    |Inner Richmond                |4.364728709292966|
    |Financial District/South Beach|4.344084616885593|
    |Haight Ashbury                |4.266428579390049|
    +------------------------------+-----------------+
    only showing top 10 rows

    Neighborhood in San Francisco with the worst response time to fire calls in 2018: Chinatown

    '''

    ### Question 5: Which week in the year in 2018 had the most fire calls?

    #fire_df = fire_df.withColumn("CallDate", to_date(col("CallDate"), "MM/dd/yyyy"))
    
    # Checking Week in 2018 had most fire calls and print result
    #fire_calls_week = fire_df.filter(year("CallDate") == 2018).withColumn("WeekOfYear", weekofyear("CallDate")).groupBy("WeekOfYear").count().orderBy("count", ascending=False)

    #fire_calls_week.show(10, truncate=False)

    # Get the week of the year with the highest count
    #highest_fire_calls_week = fire_calls_week.select("WeekOfYear").first()[0]

    # Show the result
    #print("\nWeek in 2018 with the most fire calls:", highest_fire_calls_week)

    '''
    +----------+-----+
    |WeekOfYear|count|
    +----------+-----+
    |22        |259  |
    |40        |255  |
    |43        |250  |
    |25        |249  |
    |1         |246  |
    |44        |244  |
    |13        |243  |
    |32        |243  |
    |11        |240  |
    |18        |236  |
    +----------+-----+
    only showing top 10 rows

    Week in 2018 with the most fire calls: 22

    '''
    #### Question 6: Is there a correlation between neighborhood, zip code, and number of fire calls?

    #fire_df = fire_df.withColumn("NumAlarms", col("NumAlarms").cast(IntegerType()))
    
    # Calculate correlation between zipcode and fire call count grouped by Neighborhood
    #correlation_df = fire_df.groupBy("Neighborhood").agg(corr("zipcode", "NumAlarms").alias("Zipcode_NumAlarms_Correlation")).na.drop(subset=["Zipcode_NumAlarms_Correlation"])

    # Show the correlation
    #print("\nCorrelation between Zipcode and NumAlarms for each neighbourhood:")
    #correlation_df.show(10, truncate=False)


    '''
    Correlation between Zipcode and NumAlarms:
    +------------------------------+-----------------------------+
    |Neighborhood                  |Zipcode_NumAlarms_Correlation|
    +------------------------------+-----------------------------+
    |Inner Sunset                  |-0.004476898486203728        |
    |Haight Ashbury                |0.006091300934654958         |
    |Japantown                     |-0.06396592075665322         |
    |North Beach                   |-0.15990112056099146         |
    |Lone Mountain/USF             |0.01868239360321785          |
    |Western Addition              |0.031140359689960585         |
    |Bernal Heights                |-0.010296855522968142        |
    |Mission Bay                   |0.040865789371864525         |
    |Hayes Valley                  |0.02270512498292246          |
    |Financial District/South Beach|-0.0038474374804565153       |
    +------------------------------+-----------------------------+
    only showing top 10 rows


    '''
   
    ### Question 7: How can we use Parquet files or SQL tables to store this data and read it back?

    # Write the DataFrame 'fire_df' to Parquet format with the specified mode (overwrite)
    #fire_df.write.parquet("output.parquet", mode="overwrite")

    # Read the Parquet file 'output.parquet' into a new DataFrame 'fire_parquet'
    #sf_fire_parquet = spark.read.parquet("output.parquet")
    
    # Display the first 5 rows of the DataFrame 'sf_fire_parquet' with the option to show all columns without truncation
    #sf_fire_parquet.show(n=5, truncate=False)

    '''
    +----------+------+--------------+----------------+----------+----------+--------------------+----------------------+---------------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+---------------------+-------------------------------------+-------------+---------+----+
    |CallNumber|UnitID|IncidentNumber|CallType        |CallDate  |WatchDate |CallFinalDisposition|AvailableDtTm         |Address                    |City|Zipcode|Battalion|StationArea|Box |OriginalPriority|Priority|FinalPriority|ALSUnit|CallTypeGroup|NumAlarms|UnitType|UnitSequenceInCallDispatch|FirePreventionDistrict|SupervisorDistrict|Neighborhood         |Location                             |RowID        |Delay    |Year|
    +----------+------+--------------+----------------+----------+----------+--------------------+----------------------+---------------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+---------------------+-------------------------------------+-------------+---------+----+
    |20110016  |T13   |2003235       |Structure Fire  |01/11/2002|01/10/2002|Other               |01/11/2002 01:51:44 AM|2000 Block of CALIFORNIA ST|SF  |94109  |B04      |38         |3362|3               |3       |3            |false  |NULL         |1        |TRUCK   |2                         |4                     |5                 |Pacific Heights      |(37.7895840679362, -122.428071912459)|020110016-T13|2.95     |2002|
    |20110022  |M17   |2003241       |Medical Incident|01/11/2002|01/10/2002|Other               |01/11/2002 03:01:18 AM|0 Block of SILVERVIEW DR   |SF  |94124  |B10      |42         |6495|3               |3       |3            |true   |NULL         |1        |MEDIC   |1                         |10                    |10                |Bayview Hunters Point|(37.7337623673897, -122.396113802632)|020110022-M17|4.7      |2002|
    |20110023  |M41   |2003242       |Medical Incident|01/11/2002|01/10/2002|Other               |01/11/2002 02:39:50 AM|MARKET ST/MCALLISTER ST    |SF  |94102  |B03      |01         |1455|3               |3       |3            |true   |NULL         |1        |MEDIC   |2                         |3                     |6                 |Tenderloin           |(37.7811772186856, -122.411699931232)|020110023-M41|2.4333334|2002|
    |20110032  |E11   |2003250       |Vehicle Fire    |01/11/2002|01/10/2002|Other               |01/11/2002 04:16:46 AM|APPLETON AV/MISSION ST     |SF  |94110  |B06      |32         |5626|3               |3       |3            |false  |NULL         |1        |ENGINE  |1                         |6                     |9                 |Bernal Heights       |(37.7388432849018, -122.423948785199)|020110032-E11|1.5      |2002|
    |20110043  |B04   |2003259       |Alarms          |01/11/2002|01/10/2002|Other               |01/11/2002 06:01:58 AM|1400 Block of SUTTER ST    |SF  |94109  |B04      |03         |3223|3               |3       |3            |false  |NULL         |1        |CHIEF   |2                         |4                     |2                 |Western Addition     |(37.7872890372638, -122.424236212664)|020110043-B04|3.4833333|2002|
    +----------+------+--------------+----------------+----------+----------+--------------------+----------------------+---------------------------+----+-------+---------+-----------+----+----------------+--------+-------------+-------+-------------+---------+--------+--------------------------+----------------------+------------------+---------------------+-------------------------------------+-------------+---------+----+
    only showing top 5 rows

    '''

    # Stop SparkSession
    spark.stop()