// Import Spark libraries
package main.scala.assignment_01
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Create object assignment_01
object assignment_01 {
    def main(args: Array[String]){

    //Create spark session
    val spark = SparkSession
                .builder
                .appName("Divvy Trip Analysis 2015")
                .getOrCreate()
    if (args.length < 1){
        println("Usage: DivvyTripData <file>")
        sys.exit(-1)
    }

    // get divvy data filename and convert to spark dataframe
    val divvy_data = args(0)
    println(s"Divvy Schema Inferred:")
    val df_inferred = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(divvy_data)
    df_inferred.printSchema()

    // Print the total Records
    println(s"Total Row Count: ${df_inferred.count()}")

    //define the schema by program
    println(s"Divvy Schema defined programatically in Scala:")
    val divvy_schema = StructType(Array(
        StructField("trip_id", IntegerType, true),
        StructField("starttime", StringType, true),
        StructField("stoptime", StringType, true),
        StructField("bikeid", IntegerType, true),
        StructField("tripduration", IntegerType, true),
        StructField("from_station_id", IntegerType, true),
        StructField("from_station_name", StringType, true),
        StructField("to_station_id", IntegerType, true),
        StructField("to_station_name", StringType, true),
        StructField("usertype", StringType, true),
        StructField("gender", StringType, true),
        StructField("birthyear", IntegerType, true)
    ))

    // Read the csv file with above defined schema
    val df_divvy_struct = spark.read.format("csv")
        .option("header", "true")
        .schema(divvy_schema)
        .load(divvy_data)
    df_divvy_struct.printSchema()

    // Print the total Records
    println(s"Total Row Count: ${df_divvy_struct.count()}")

    // Define schema in DDL
    println(s"Divvy Schema defined using DDL:")
    val divvy_schema_ddl = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, usertype STRING, gender STRING, birthyear INT"

    // Read the csv file with schema defined in DDL
    val df_divvy_ddl = spark.read.format("csv")
        .option("header", "true")
        .schema(divvy_schema_ddl)
        .load(divvy_data)
    df_divvy_ddl.printSchema()

    // Print the total Records
    println(s"Total Row Count: ${df_divvy_ddl.count()}")

    // Filter records based on Gender
    val df_gender_female = df_divvy_ddl.select("*")
        .where(col("gender") === "Female")
        .groupBy("to_station_name")
        .count()
    
    // Show 10 records from the grouped results
    println(s"Records after grouping:")
    df_gender_female.show(10, truncate = false)

    //Stop Spark session
    spark.stop()
    }
}