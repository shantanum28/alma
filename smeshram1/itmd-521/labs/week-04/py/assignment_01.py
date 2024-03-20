import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder \
    .appName("Python Spark Schema Assignment") \
    .getOrCreate()

# Get file path from command line arguments
file_path = sys.argv[1] if len(sys.argv) > 1 else "default/path/to/csv/file.csv"

# First read: Infer schema
df1 = spark.read.option("header", "true").csv(file_path)
print("Inferred Schema:")
df1.printSchema()
print("Number of records:", df1.count())

# Second read: Programmatically define schema
schema =StructType([
        StructField("trip_id", IntegerType(),True),
        StructField("starttime",StringType(),False),
        StructField("stoptime",StringType(),True),
        StructField("bikeid", IntegerType(),True),
        StructField("tripduration", IntegerType(),True),
        StructField("from_station_id", IntegerType(),True),
        StructField("from_station_name", StringType(),False),
        StructField("to_station_id", IntegerType(),True),
        StructField("to_station_name", StringType(),True),
        StructField("usertype", StringType(),False),
        StructField("gender", StringType(),True),
        StructField("birthyear", IntegerType(),True)])
    # Define schema for other columns as needed
df2 = spark.read.schema(schema).option("header", "true").csv(file_path)
print("Programmatically Defined Schema:")
df2.printSchema()
print("Number of records:", df2.count())

# Third read: Schema via DDL
ddl_schema = "trip_id INTEGER,starttime STRING,stoptime STRING,bikeid INTEGER,tripduration INTEGER,from_station_id INTEGER,from_station_name STRING,to_station_id INTEGER,to_station_name STRING,usertype STRING ,gender STRING,birthyear INTEGER "
df3 = spark.read.schema(ddl_schema).option("header", "true").csv(file_path)
print("Schema via DDL:")
df3.printSchema()
print("Number of records:", df3.count())

gender_df = (df1.select("*")
                       .where(df1.gender == 'Female')
                       .groupBy("to_station_name")
                       .count())
gender_df.show(n=10, truncate=False)

# Stop SparkSession
spark.stop()