from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("Assignment01").getOrCreate()

# Define file path
csv_path = "/home/vagrant/sshah215/itmd-521/labs/week-4/Divvy_Trips_2015-Q1.csv"

# First: Read with inferred schema
df_inferred = spark.read.option("header", "true").csv(csv_path)
print("DataFrame with Inferred Schema:")
df_inferred.printSchema()
print("Number of Records:", df_inferred.count())

# Second: Read with programmatically defined schema
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
df_programmatic = spark.read.option("header", "true").schema(schema).csv(csv_path)
print("\nDataFrame with Programmatically Defined Schema:")
df_programmatic.printSchema()
print("Number of Records:", df_programmatic.count())

# Third: Read with schema defined via DDL
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
df_ddl = spark.read.option("header", "true").option("inferSchema", "false").schema(ddl_schema).csv(csv_path)
print("\nDataFrame with Schema Defined via DDL:")
df_ddl.printSchema()
print("Number of Records:", df_ddl.count())

# Stop Spark session
spark.stop()