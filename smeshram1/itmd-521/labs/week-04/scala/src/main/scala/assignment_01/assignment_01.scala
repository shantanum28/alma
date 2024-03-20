import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object Assignment01 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder.appName("Assignment01").getOrCreate()

    // Define file path
    val csvPath = "/home/vagrant/sshah215/itmd-521/labs/week-4/Divvy_Trips_2015-Q1.csv"

    // Step 1: Read CSV with inferred schema
    val df1 = spark.read.option("header", "true").csv(csvPath)
    printSchemaAndCount("Inferred Schema", df1)

    // Show 10 records
    df1.show(10)

    // Step 2: Programmatically use StructFields to create and attach a schema
    val schema = StructType(
      StructField("col1", StringType, true) ::
      StructField("col2", IntegerType, true) ::
      // Add more fields based on your CSV structure
      Nil
    )
    val df2 = spark.read.option("header", "true").schema(schema).csv(csvPath)
    printSchemaAndCount("Programmatic Schema", df2)

    // Show 10 records
    df2.show(10)

    // Step 3: Attach a schema via DDL and read the CSV file
    val ddlSchema = "col1 STRING, col2 INT"  // Define DDL schema based on your CSV structure
    val df3 = spark.read.option("header", "true").option("inferSchema", "false").schema(ddlSchema).csv(csvPath)
    printSchemaAndCount("DDL Schema", df3)

    // Show 10 records
    df3.show(10)

    // Transformations and Actions
    val selectedDF = df1.select("Gender").filter(df1("LastName").startsWith("A-K")).filter(df1("Gender") === "Female")
    val groupedDF = selectedDF.groupBy("station_name").count()

    // Show 10 records
    groupedDF.show(10)

    // Stop Spark session
    spark.stop()
  }

  def printSchemaAndCount(message: String, df: DataFrame): Unit = {
    println(s"$message Schema:")
    df.printSchema()
    println(s"Count: ${df.count()}\n")
  }
}