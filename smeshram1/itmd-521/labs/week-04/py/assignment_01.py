from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Step 1: Create Spark Session & Read the Divvy_Trips_2015-Q1.csv file and infer the schema
spark = SparkSession.builder.appName("DivvyTrips").getOrCreate()
df_inferred = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, inferSchema=True)

# Step 2: Print the inferred schema
print("Inferred Schema:")
df_inferred.printSchema()

# Step 3: Programmatically use StructFields to create and attach a schema
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("starttime", StringType(), True),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), True),
    StructField("tripduration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

df_programmatic = spark.read.csv("Divvy_Trips_2015-Q1.csv", header=True, schema=schema)

# Print the programmatically attached schema
print("\nProgrammatically Attached Schema:")
df_programmatic.printSchema()

# Step 4: Attach a schema via DDL and read the CSV file
ddl_schema = """
    trip_id INT,
    starttime STRING,
    stoptime STRING,
    bikeid INT,
    tripduration INT,
    from_station_id INT,
    from_station_name STRING,
    to_station_id INT,
    to_station_name STRING,
    usertype STRING,
    gender STRING,
    birthyear INT
"""

df_ddl = spark.read.option("header", "true").option("inferSchema", "false").schema(ddl_schema).csv("Divvy_Trips_2015-Q1.csv")

# Print the DDL attached schema
print("\nDDL Attached Schema:")
df_ddl.printSchema()

# Step 5: Use .count() to display the number of records in each DataFrame
print("\nNumber of Records in Each DataFrame:")
print("Inferred Schema DataFrame Count:", df_inferred.count())
print("Programmatically Attached Schema DataFrame Count:", df_programmatic.count())
print("DDL Attached Schema DataFrame Count:", df_ddl.count())

#Step 6: Transformation and Action

# Register DataFrame as a temporary view
df_inferred.createOrReplaceTempView("divvy_trips")

# Execute the SQL query 
result = spark.sql("""
    SELECT gender, to_station_name, count(*) as count
    FROM divvy_trips 
    WHERE gender = 'Female' 
    GROUP BY gender, to_station_name
    ORDER BY count DESC
""")

# Show the top 10 records
result.show(10)


# Stop the SparkSession
spark.stop()