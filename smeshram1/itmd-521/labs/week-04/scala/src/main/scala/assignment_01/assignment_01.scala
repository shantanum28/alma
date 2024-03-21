import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object assignment_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]") 
      .appName("Divvy Trips")
      .getOrCreate()

    // Define schema programmatically
    val schema = new StructType()
      .add("trip_id", IntegerType, nullable = true)
      .add("starttime", StringType, nullable = true)
      .add("stoptime", StringType, nullable = true)
      .add("bikeid", IntegerType, nullable = true)
      .add("tripduration", IntegerType, nullable = true)
      .add("from_station_id", IntegerType, nullable = true)
      .add("from_station_name", StringType, nullable = true)
      .add("to_station_id", IntegerType, nullable = true)
      .add("to_station_name", StringType, nullable = true)
      .add("usertype", StringType, nullable = true)
      .add("gender", StringType, nullable = true)
      .add("birthyear", IntegerType, nullable = true)

    // Read CSV with inferred schema
    val dfInferred = spark.read
      .option("header", "true")
      .csv("Divvy_Trips_2015-Q1.csv")
    println("Inferred Schema:")
    dfInferred.printSchema()
    println("Number of records:", dfInferred.count())

    // Read CSV with programmatically defined schema
    val dfProgrammatic = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("Divvy_Trips_2015-Q1.csv")
    println("\nProgrammatic Schema:")
    dfProgrammatic.printSchema()
    println("Number of records:", dfProgrammatic.count())

    // Read CSV with schema via DDL
    val dfDDL = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("Divvy_Trips_2015-Q1.csv")
    println("\nSchema via DDL:")
    dfDDL.printSchema()
    println("Number of records:", dfDDL.count())

    spark.stop()
  }
}