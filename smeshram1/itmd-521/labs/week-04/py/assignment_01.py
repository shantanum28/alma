#First infer the schema and read the csv file
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
if __name__ == "__main__":
      if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
      spark = (SparkSession
      .builder
      .appName("Divya_Trips")
      .getOrCreate())
      fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
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
      csv_file = sys.argv[1]
      sampleDF = spark.read.option("samplingRatio", 0.001) .option("header", True) .csv(csv_file)
      print(sampleDF.printSchema())


      schema2 = "trip_id STRING, starttime STRING, stoptime STRING, bikeid STRING, tripduration STRING, from_station_id STRING, from_station_name STRING, to_station_id STRING, to_station_name STRING, usertype STRING, gender STRING, birthyear STRING"
      csv_file = sys.argv[1]
      sampleDF = spark.read.option("samplingRatio", 0.001) .option("header", True) .csv(csv_file)
      data_frame=spark.read.csv(csv_file, header=True, schema=schema)
      data_frame2=spark.read.csv(csv_file, header=True, schema=schema2)
      print(sampleDF.printSchema())
      print(sampleDF.count())
      print(data_frame.printSchema())
      print(data_frame.count())
      print(data_frame2.printSchema())
      print(data_frame2.count())
      final_data_frame = spark .read.option("samplingRatio", 0.001) .option("header", True) .csv(csv_file)
      (final_data_frame.select("gender","to_station_name").where(final_data_frame.gender=="Female").groupBy("gender","to_station_name").count().show(10,truncate=False))