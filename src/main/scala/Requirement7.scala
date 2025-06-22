import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Requirement7{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    //extract year and count number of sales per year
    val yearwiseSalesDF = salesDF
      // Parse date
      .withColumn("Order_Date_Parsed", to_date(col("Order Date"), "M/d/yyyy"))
      // Extract year
      .withColumn("Year", year(col("Order_Date_Parsed")))
      // Filter out null years
      .filter(col("Year").isNotNull)
      // Group and count
      .groupBy("Year").agg(count("*").as("Number_of_Sales")).orderBy(col("Year"))    // ascending order
    yearwiseSalesDF.show()
    yearwiseSalesDF.write.parquet("C:/Users/gomall/Desktop/requirement7.parquet")
    spark.stop()
  }

}

object Requirement6{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    val itemsPriceCostDF = salesDF1.select("Item Type", "Unit Price", "Unit Cost").orderBy(asc("Unit Price"), asc("Unit Cost"))

    // Show the result
    itemsPriceCostDF.show()
    itemsPriceCostDF.write.parquet("C:/Users/gomall/Desktop/requirement6.parquet")
    spark.stop()
  }

}

object Requirement5{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    val regionCountryUnitsDF = salesDF1.groupBy("Region", "Country").agg(sum("Units Sold").as("Total_Units_Sold"))
    // Define a window partitioned by region, ordered by Total_Units_Sold desc
    val windowSpec = Window.partitionBy("Region").orderBy(desc("Total_Units_Sold"))
    // Pick top country per region
    val topCountryPerRegionDF = regionCountryUnitsDF.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") === 1)
      .select("Region", "Country", "Total_Units_Sold")
    topCountryPerRegionDF.show()
    topCountryPerRegionDF.write.parquet("C:/Users/gomall/Desktop/requirement5.parquet")
    spark.stop()
  }

}

object Requirement4{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    val productsWithTwoAs = salesDF1.select("Item Type").distinct()
      .filter(length(regexp_replace(col("Item Type"), "[^aA]", "")) >= 2)
    productsWithTwoAs.show()
    productsWithTwoAs.write.parquet("C:/Users/gomall/Desktop/requirement4.parquet")
    spark.stop()
  }

}

object Requirement3{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    val mostRecentSalesDF = salesDF1.withColumn("Order_Date_Parsed",to_date(col("Order Date"), "M/d/yyyy"))
      .orderBy(desc("Order_Date_Parsed"))  // Order by most recent
      .limit(10)
    mostRecentSalesDF.show()

    mostRecentSalesDF.write.parquet("C:/Users/gomall/Desktop/requirement3.parquet")
    spark.stop()
  }

}

object Requirement2{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    val unitsSoldByRegionDF = salesDF1.groupBy("Region").agg(sum("Units Sold").as("Total_Units_Sold"))
      .orderBy(desc("Total_Units_Sold"))
    unitsSoldByRegionDF.show()
    unitsSoldByRegionDF.write.parquet("C:/Users/gomall/Desktop/requirement2.parquet")
    spark.stop()
  }

}

object Requirement1{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    val numCountriesDF = salesDF
      .agg(countDistinct("Country").as("Number_of_Countries"))
    numCountriesDF.show()
    numCountriesDF.write.parquet("C:/Users/gomall/Desktop/requirement1.parquet")
    spark.stop()
  }

}