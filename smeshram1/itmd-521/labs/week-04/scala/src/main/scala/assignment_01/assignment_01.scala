import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}
import org.apache.spark.sql.functions._

object assignment_01 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: assignment_01.scala <file>")
      sys.exit(-1)
    }

    val spark = SparkSession.builder.appName("assignment_01").getOrCreate()

    val filePath = args(0)

    // Inferred schema
    val df1 = spark.read.option("header", "true").csv(filePath)
    println("DataFrame 1 - Inferred Schema:")
    df1.printSchema()
    println("Number of Records: " + df1.count())

    // Programmatically defined schema
    val schema = new StructType()
      .add(StructField("trip_id", IntegerType, true))
      .add(StructField("starttime", StringType, true))
      .add(StructField("stoptime", StringType, true))
      .add(StructField("bikeid", IntegerType, true))
      .add(StructField("tripduration", IntegerType, true))
      .add(StructField("from_station_id", IntegerType, true))
      .add(StructField("from_station_name", StringType, true))
      .add(StructField("to_station_id", IntegerType, true))
      .add(StructField("to_station_name", StringType, true))
      .add(StructField("usertype", StringType, true))
      .add(StructField("gender", StringType, true))
      .add(StructField("birthyear", IntegerType, true))

    val df2 = spark.read.option("header", "true").schema(schema).csv(filePath)
    println("\nDataFrame 2 - Programmatically Defined Schema:")
    df2.printSchema()
    println("Number of Records: " + df2.count())

    // DDL-defined schema
    val ddlSchema = "trip_id INT, starttime STRING, stoptime STRING, bikeid INT, tripduration INT, " +
      "from_station_id INT, from_station_name STRING, to_station_id INT, to_station_name STRING, " +
      "usertype STRING, gender STRING, birthyear INT"

    val df3 = spark.read.option("header", "true").option("inferSchema", "false").schema(ddlSchema).csv(filePath)
    println("\nDataFrame 3 - DDL Defined Schema:")
    df3.printSchema()
    println("Number of Records: " + df3.count())

    val lastName = "meshram"
    val lastNameStartsWithAToK = 'A' <= lastName.toUpperCase.charAt(0) && lastName.toUpperCase.charAt(0) <= 'K'
    val selectedGender = if (lastNameStartsWithAToK) "Female" else "Male"
    println(s"\nSelected Gender: $selectedGender")

    val filteredDF = df3.filter(col("gender") === selectedGender)

    val groupedDF: DataFrame = filteredDF.groupBy("to_station_name").count()

    println("\nGroupBy to_station_name and Display 10 Records:")
    groupedDF.show(10, truncate = false)

    spark.stop()
  }
}
