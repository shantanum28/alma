import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "_main_":
    if len(sys.argv) <= 0:
        sys.exit(1)

    spark = SparkSession.builder.appName("Divvy_Trip").getOrCreate()
    # Source file
    csvFile = sys.argv[1]
    
    # Infer the Schema
    inferDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csvFile)
    print("**************************************")
    inferDataFrame.show(n=10)
    print("***************Schema inferred **************")
    inferDataFrame.printSchema()
    print("**************Number of records*****************")        
    print("Number of records in infer DataFrame = " + str(inferDataFrame.count()))
    print("****************End of infer schema***************")
    
    # Defining schema programmatically 
    schema = StructType([
        StructField("trip_id", IntegerType()),
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
        StructField("birthyear", IntegerType())
    ])
    
    structDataFrame = spark.read.schema(schema).csv(csvFile)
    print("**************************************")
    structDataFrame.show(n=10)
    print("************Print Schema Programmatically*************")
    structDataFrame.printSchema()
    print("**************Number of records*******************")
    print("Number of records in structure DataFrame = " + str(structDataFrame.count()))
    print("****************End of structured schema***************")

    
    # Attaching a schema via DDL
    ddlSchema = "trip_id INT,starttime STRING,stoptime STRING,bikeid INT,tripduration INT,from_station_id INT,from_station_name STRING,to_station_id INT,to_station_name STRING,usertype STRING,gender STRING,birthyear INT"
    ddlDataFrame = spark.read.schema(schema).csv(csvFile)
    print("**************************************")
    ddlDataFrame.show(n=10)
    print("************Print Schema DDL ******************")
    ddlDataFrame.printSchema()
    print("**************Number of records*****************")
    print("Number of records in ddl DataFrame = " + str(ddlDataFrame.count()))
    print("****************End of ddl schema*****************")