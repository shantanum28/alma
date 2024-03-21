# Import Spark libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, count

# main block
if __name__ == "__main__":
        if len(sys.argv) != 2:
           print("Usage: assignment-01.py <enter correct path to divvy dataset>", file=sys.stderr)
           sys.exit(-1)

        #Initialize Spark Session
        spark = (SparkSession
                .builder
                .appName("DivvyTripsAnalysis2015")
                .getOrCreate())

        #Get Divvy data file path
        print("Divvy Schema inferred:")
        divvy_data = sys.argv[1]
        df_inferred = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(divvy_data))
        df_inferred.printSchema()

        # Print the total Records
        print("Total Row Count:", df_inferred.count())

        #define the schema by program
        print("Divvy Schema defined programatically:")
        divvy_schema = StructType([
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
        StructField("birthyear", IntegerType(), True)])

        # Read the csv file with above defined schema
        df_divvy_struct = (spark.read.format("csv")
        .option("header", "true")
        .schema(divvy_schema)
        .load(divvy_data))
        df_divvy_struct.printSchema()

        # Print the total Records
        print("Total Row Count:", df_divvy_struct.count())

        # define schema in DDL
        print("Divvy Schema defined using DDL:")
        divvy_schema_ddl = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

        # Read the csv file with schema defined in DDL
        df_divvy_ddl = (spark.read.format("csv")
        .option("header", "true")
        .schema(divvy_schema_ddl)
        .load(divvy_data))
        df_divvy_ddl.printSchema()

        # Print the total Records
        print("Total Row Count:", df_divvy_ddl.count())

        # Filter records based on Gender
        df_gender_female = (df_divvy_struct.select("*")
        .where(col("gender") == "Female")
        .groupBy("to_station_name")
        .count())

        # Show 10 records from the grouped results
        print("Records after grouping:")
        df_gender_female.show(10, truncate=False)
        
# Stop Session
spark.stop()