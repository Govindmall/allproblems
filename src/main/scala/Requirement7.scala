import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Requirement7{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/sdattake/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1).cache()
    //extract year and count number of sales per year
    val yearwiseSalesDF = salesDF1
      // Parse date
      .withColumn("Order_Date_Parsed", to_date(col("Order Date"), "M/d/yyyy"))
      // Extract year
      .withColumn("Year", year(col("Order_Date_Parsed")))
      // Filter out null years
      .filter(col("Year").isNotNull)
      // Group and count
      .groupBy("Year").agg(count("*").as("Number_of_Sales")).orderBy(col("Year"))    // ascending order
    yearwiseSalesDF.show()
    yearwiseSalesDF.write.parquet("C:/Users/sdattake/Desktop/requirement7.parquet")
    spark.stop()
  }

}

