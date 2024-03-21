import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: DivvyTrips <file>", file=sys.stderr)
        sys.exit(-1)
    
    spark = (SparkSession
    .builder
    .appName("Assignment1")
    .getOrCreate())
    Divvy_File = sys.argv[1]
    
    DataFrame_CSV = (spark.read.format("csv")
    .option("header","true")
    .option("inferSchema", "true")
    .load(Divvy_File))
    DataFrame_CSV.show(n=5, truncate=False)
    DataFrame_CSV.printSchema()
    #parquet_path = DataFrame_CSV.write.format("parquet").save("/home/vagrant/gdisale/assignment-01/example-data/Divvy-2015")
    Parquert_File = spark.read.format("parquet").load("/home/vagrant/gdisale/assignment-01/example-data/Divvy-2015")
    Parquert_File.show(n=5, truncate=False)


    ## Using StructType below

    gSchema = StructType([StructField("trip_id", IntegerType()),
      StructField("starttime", StringType()),
      StructField("stoptime", StringType()),
      StructField("bikeid", IntegerType()),
      StructField("tripduration", IntegerType()),
      StructField("from_station_id", IntegerType()),
      StructField("from_station_name", StringType()),
      StructField("to_station_id", IntegerType()),
      StructField("to_station_name", StringType()),
      StructField("usertype", StringType()),
      StructField("gender", StringType()),
      StructField("birthyear", IntegerType())])
    
    Structure_DataFrame = (spark.read.schema(gSchema).format("csv")).option("header", "true").option("structureSchema", "true").load(Divvy_File)
    Structure_DataFrame.show()
    Structure_DataFrame.printSchema()

    ## using Data Definition Language 

    data_gschema = "trip_id INT,starttime STRING,stoptime STRING,bikeid INT,tripduration INT,from_station_id INT,from_station_name STRING,to_station_id INT,to_station_name STRING,usertype STRING,gender STRING,birthyear INT"
    blogsDataFrame = (spark.read.schema(data_gschema).format("csv")).option("header", "true").load(Divvy_File)
    blogsDataFrame.show()
    blogsDataFrame.printSchema()
    gender_dataframe = (DataFrame_CSV.select("gender", "to_station_id", "to_station_name").where(DataFrame_CSV.gender == 'Female').groupBy("to_station_id").count())
    gender_dataframe.show(n=10, truncate=False)