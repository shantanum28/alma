import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.functions import col, year,month, to_timestamp, desc, weekofyear

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment_02 <file>", file=sys.stderr)
        sys.exit(-1)

# Build a SparkSession
spark = (SparkSession
    .builder
    .appName("TypesOfFireCalls")
    .getOrCreate())
# Get the filename from the command-line arguments
fire_calls_file = sys.argv[1]
# Programmatic way to define a schema 
sffire_schema = StructType([StructField('CallNumber', IntegerType(), True),
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
# Read the file into a Spark DataFrame
fire_calls_df = (spark.read.format("csv") 
    .option("header", "true") 
    .schema(sffire_schema ) 
    .load(fire_calls_file))
# Converting the CallDate column to timestamp format
updated_fire_calls_df = fire_calls_df.withColumn("CallingDate", to_timestamp(fire_calls_df["CallDate"], "MM/dd/yyyy")).drop("CallDate")
# Q1. What were all the different types of fire calls in 2018?
# Filtering for fire calls in 2018
fire_calls_2018_updated = (updated_fire_calls_df
    .filter(year("CallingDate") == 2018)
    .select("CallType").distinct())
# Get different types of fire calls in 2018
print("Output Q1: Different types of fire calls in 2018")
fire_calls_2018_updated.show(truncate=False)

# Output for Question1
# CallType                       
# Elevator / Escalator Rescue    
# Alarms                         
# Odor (Strange / Unknown)       
# Citizen Assist / Service Call  
# HazMat                         
# Vehicle Fire                   
# Other                          
# Outside Fire                   
# Traffic Collision              
# Assist Police                  
# Gas Leak (Natural and LP Gases)
# Water Rescue                   
# Electrical Hazard              
# Structure Fire                 
# Medical Incident               
# Fuel Spill                     
# Smoke Investigation (Outside)  
# Train / Rail Incident          
# Explosion                      
# Suspicious Package             

# Q2.What months within the year 2018 saw the highest number of fire calls?
fire_calls_by_month = (fire_calls_df
    .withColumn("CallingDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .withColumn("Year", year(col("CallingDate")))
    .filter(col('Year') == 2018)
    .withColumn("Month", month(col("CallingDate")))
    .groupBy("Month")
    .count())
# Find the month with the highest number of fire calls and its count
highest_call_count = fire_calls_by_month.orderBy(
    ("count")).limit(1)
# Display the count of the highest number of fire calls
print("Output Q2:Count of the highest number of fire calls")
highest_call_count.show(truncate=False)

# Output for Question2
# Month with the highest number of fire calls in 2018: 10 
# count: 1068

# Q3.Which neighborhood in San Francisco generated the most fire calls in 2018?
fire_calls_by_neighborhood = (fire_calls_df
    .filter((col("City") == "San Francisco") | (col("City") == "SF") | (col("City") == "SFO"))
    .withColumn("Year", fire_calls_df["CallDate"].substr(7, 4).cast("int"))
    .filter(col('Year') == 2018)
    .groupBy("Neighborhood")
    .count())
# Find the neighborhood with the highest number of fire calls
neighborhood_with_highest_calls = fire_calls_by_neighborhood.orderBy(col("count").desc())
# Display the neighborhood with the highest number of fire calls
print("Output Q3:Neighborhood with the highest number of fire calls")
neighborhood_with_highest_calls.show(10,truncate=False)

 
# Output for Question3
# Neighborhood with the most fire calls in 2018:Top Neighborhoods: Tenderloin, South of Market, Mission, Financial District/South Beach, Bayview Hunters Point, Western Addition, Sunset/Parkside, Nob Hill, Hayes Valley, Outer Richmond
# Highest Calls: Tenderloin with 1393 calls

# Q4.Which neighborhoods had the worst response times to fire calls in 2018? 
worst_response_time_neighborhoods = (fire_calls_df
    .withColumn("CallingDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .withColumn("Year", year(col("CallingDate")))
    .filter(col('Year') == 2018)
    .groupBy("Neighborhood")
    .agg({'Delay': 'avg'})
    .orderBy("avg(Delay)", ascending = False))
# Neighborhoods with the worst response times to fire calls in 2018
print("Output Q4:Neighborhoods with the worst response times to fire calls in 2018")
worst_response_time_neighborhoods.show(10,truncate=False)

# Output for Question4
# The neighborhoods with highest worst response time to fire calls in 2018- Chinatown, Presidio, Treasure Island, McLaren Park, Bayview Hunters Point, Presidio Heights, Inner Sunset, Inner Richmond, Financial District/South Beach, Haight Ashbury
# Worst Response Time: Chinatown with an average delay of 6.19 minutes

# Q5.Which week in the year in 2018 had the most fire calls?
most_fire_calls_week = (fire_calls_df
    .withColumn("CallingDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .withColumn("WeekOfYear", weekofyear(col("CallingDate")))
    .filter(year("CallingDate") == 2018)
    .groupBy("WeekOfYear")
    .count()
    .orderBy("count", ascending = False)
    .limit(1))
# Display the week with the most fire calls in 2018
print("Output Q5:Week in 2018 with the most fire calls")
most_fire_calls_week.show(truncate=False)

# Output for Question5
# In the year 2018, Week 22 had the highest number of fire calls, with a count of 259 calls.

# Q6.Is there a correlation between neighborhood, zip code, and number of fire calls?
correlation = (fire_calls_df
    .groupBy("Neighborhood","Zipcode")
    .count())
# Display the correlation between neighborhood, zip code, and number of fire calls
print("Output Q6:Correlation between neighborhood, zip code and number of fire calls:")
correlation.show(10,truncate=False)
print()

# Output for Question6
# Neighborhoods: Inner Sunset, Bayview Hunters Point, West of Twin Peaks, Haight Ashbury, Glen Park, Excelsior, Russian Hill, None, Chinatown
# Zip Codes: 94122, 94124, 94114, 94112, 94110, 94109, 94133
# Highest Calls: Inner Sunset with 2161 calls (Zip code: 94122)

# Q6.How can we use Parquet files or SQL tables to store this data and read it back?
fire_calls_df_file_parquet= "~/LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.parquet"
fire_calls_df.write.mode("overwrite").parquet(fire_calls_df_file_parquet)
fire_calls_df_read_parquet= (spark
    .read
    .format("parquet")
    .load(fire_calls_df_file_parquet))
print()
print("Output Q7:Parquet files to store this data and read it back")
print()
fire_calls_df_read_parquet.show(7, truncate=False)

# Output for Question7
# In the output it is reading and writing the data mentioned in the csv and the  it is displaying all the headers mentioned in the csv and date of 7 rows.

# Stop SparkSession
spark.stop()