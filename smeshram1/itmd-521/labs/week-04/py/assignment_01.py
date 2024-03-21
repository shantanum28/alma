# In python lab02
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = ( SparkSession
    .builder
    .appName("Lab2")
    .getOrCreate())
df = spark.read.csv("./data/Divvy_Trips_2015-Q1.csv", header=True)
df.printSchema()
print("\nNumber of Rows:", df.count())

schema = StructType([
    StructField("trip_id",StringType(),True),
    StructField("starttime",StringType(),True),
    StructField("stoptime",StringType(),True),
    StructField("bikeid",StringType(),True),
    StructField("tripduration",StringType(),True),
    StructField("from_station_id",StringType(),True),
    StructField("from_station_name",StringType(),True),
    StructField("to_station_id",StringType(),True),
    StructField("to_station_name",StringType(),True),
    StructField("usertype",StringType(),True),
    StructField("gender",StringType(),True),
    StructField("birthyear",StringType(),True),
])

df_with_schema = spark.createDataFrame(df.rdd,schema)

df_with_schema.show()
print("\nNumber of Rows:", df_with_schema.count())




schema_ddl = "trip_id STRING, starttime STRING, stoptime STRING,bikeid STRING , tripduration  STRING ,from_station_id STRING,  \
    from_station_name STRING,to_station_id STRING,to_station_name STRING,usertype STRING,gender STRING, birthyear STRING"

df_ddl = spark.read.csv("./data/Divvy_Trips_2015-Q1.csv", header=True, schema=schema_ddl)
df_ddl.show()
print("\nNumber of Rows:", df_ddl.count())

format_df=df_ddl.select("*").where(df_ddl["gender"] == "Male").groupBy("to_station_name").agg(count("to_station_name"))
format_df.show(10)


spark.stop()