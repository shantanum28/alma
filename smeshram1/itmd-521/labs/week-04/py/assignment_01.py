import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp

# Create SparkSession
spark = SparkSession.builder \
    .appName("Python Spark Schema") \
    .getOrCreate()

# Define file path
file_path = sys.argv[1]

# Set time parser policy to LEGACY
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# First read: Infer schema
infer_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
print("Inferred Schema:")
infer_df.printSchema()
print("Number of records:", infer_df.count())

# Second read: Programmatically define schema
programmatically_schema = StructType([
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
    StructField("gender", StringType(), False),
    StructField("birthyear", IntegerType(), False)
])

programmatically_df = spark.read.schema(programmatically_schema).option("header", "true").csv(file_path)

programmatically_df = programmatically_df.withColumn("starttime", to_timestamp(programmatically_df["starttime"], "MM/dd/yyyy HH:mm"))
programmatically_df = programmatically_df.withColumn("stoptime", to_timestamp(programmatically_df["stoptime"], "MM/dd/yyyy HH:mm"))


print("Programmatically Defined Schema:")
programmatically_df.printSchema()
print("Number of records:", programmatically_df.count())


# Third read: Schema via DDL
ddl_schema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"
ddl_df = spark.read.schema(ddl_schema).option("header", "true").csv(file_path)
ddl_schema = spark.read.schema(ddl_schema).option("header", "true").csv(file_path)

ddl_df = ddl_df.withColumn("starttime", to_timestamp(ddl_df["starttime"], "MM/dd/yyyy HH:mm"))
ddl_df = ddl_df.withColumn("stoptime", to_timestamp(ddl_df["stoptime"], "MM/dd/yyyy HH:mm"))

print("Schema via DDL:")
ddl_df.printSchema()
print("Number of records:", ddl_df.count())

#sql
gender_df = (infer_df.select("*")
    .where(infer_df.gender == 'Female')
    .groupBy("to_station_name")
    .count())

gender_df.show(n=10, truncate=False)

# Stop SparkSession
spark.stop()