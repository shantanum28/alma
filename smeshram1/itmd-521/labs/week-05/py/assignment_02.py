from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col
from pyspark.sql import *
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import count
from pyspark.sql.functions import first
from pyspark.sql.functions import avg
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import corr
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: assignment_02 <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("assignment_02")
        .getOrCreate())
    # get the M&M data set file name
    csv_file = sys.argv[1]
    # read the file into a Spark DataFrame
    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csv_file))
    df.show(10)
    df.printSchema()
    fire_new_df = (df.withColumn("NewCallDate", to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate"))
    fire_new_df.select("CallType").filter(year("NewCallDate")==2018).distinct().show(truncate=True)
    # q1 What were all the different types of fire calls in 2018?
    '''file_df = fire_new_df.select("CallType").filter(year("NewCallDate")==2018).distinct().show(truncate=True)
    '''
    ''' try parquet files '''
    

    ''' try db '''
    #spark.sql("CREATE DATABASE IF NOT EXISTS my_database")
    fire_new_df.filter(year("NewCallDate")==2018) \
            .groupBy(month("NewCallDate").alias("month")) \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc()).select("month","count").show(1,truncate=True)
    # q2 What months within the year 2018 saw the highest number of fire calls?
    fire_new_df.filter(year("NewCallDate")==2018)\
            .groupBy("Neighborhood")\
            .agg(count("*").alias("count"))\
            .orderBy(col("count").desc()).select("Neighborhood","count").show(1,truncate=True)
#q3 Which neighborhood in San Francisco generated the most fire calls in 2018?
    fire_new_df.filter(year("NewCallDate")==2018)\
            .groupBy("Neighborhood")\
            .agg(avg("Delay").alias("avg_delay"))\
            .orderBy(col("avg_delay").desc()).select("Neighborhood","avg_delay").show(1,truncate=True)
#q4 Which neighborhoods had the worst response times to fire calls in 2018?
    fire_new_df.filter(year("NewCallDate")==2018)\
            .groupBy(weekofyear("NewCallDate").alias("week"))\
            .agg(count("*").alias("count"))\
            .orderBy(col("count").desc()).select("week","count").show(1,truncate=True)
#q5 Which week in the year in 2018 had the most fire calls?
    neighborhood_zip_counts = fire_new_df.groupBy("Zipcode").agg(count("*").alias("fire_calls_count"))
    correlation_value = neighborhood_zip_counts.stat.corr("Zipcode", "fire_calls_count")
    print("Correlation between neighborhood and number of fire calls:", correlation_value)
#q6 Is there a correlation between neighborhood, zip code, and number of fire calls?
    fire_new_df.write.mode('overwrite').parquet("./q1.txt")
    file_parquet=spark.read.parquet("./q1.txt")
    file_parquet.show();
    #q7 How can we use Parquet files or SQL tables to store this data and read it back?
    spark.stop()
'''    properties = {
            "user":"pyspark",
            "password":"pyspark",
            "driver":"org.mariadb.jdbc.Driver"
    }
    jdbc_url="jdbc:mysql://127.0.0.1:3306/itmd521"
    fire_new_df.write.jdbc(url=jdbc_url,table="py_csv",mode = "overwrite",properties=properties)'''