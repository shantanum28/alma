import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .appName("PythonDivvyTrips")
             .getOrCreate())

    dt_file = "/home/vagrant/gdhore/itmd-521/labs/week-04/Divvy_Trips_2015-Q1.csv"
    dt_df = (spark.read.format("csv")
             .option("header", "True")
             .option("inferSchema", "True")
             .load(dt_file))

    print(dt_df.printSchema())

    print("Total Rows = %d" % (dt_df.count()))

    from pyspark.sql.functions import col

    dt_female = dt_df.select("*").filter(col("gender") == "Female")

    dt_grouped = dt_female.groupBy("to_station_name").count()

    dt_grouped.show(10)

    Pschema = StructType([StructField('trip_id', IntegerType(), True),
                          StructField('starttime', StringType(), True),
                          StructField('stoptime', StringType(), True),
                          StructField('bikeid', IntegerType(), True),
                          StructField('tripduration', IntegerType(), True),
                          StructField('from_station_id', IntegerType(), True),
                          StructField('from_station_name', StringType(), True),
                          StructField('to_station_id', IntegerType(), True),
                          StructField('to_station_name', StringType(), True),
                          StructField('usertype', StringType(), True),
                          StructField('gender', StringType(), True),
                          StructField('birthyear', IntegerType(), True)])

    dt_df_schema = spark.read.csv(dt_file, header=True, schema=Pschema)

    print(dt_df_schema.printSchema())
    print(dt_df_schema.schema)

    print("Total Rows = %d" % (dt_df_schema.count()))

    DDLschema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    dt_df_DDLschema = spark.read.csv(dt_file, header=True, schema=DDLschema)

    print(dt_df_DDLschema.printSchema())

    print("Total Rows = %d" % (dt_df_DDLschema.count()))

    spark.stop()