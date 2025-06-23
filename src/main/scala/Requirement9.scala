import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Requirement9{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/shemantk/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    //group by country, sum Total Revenue
    val countrySalesDF=salesDF1.groupBy("Country").agg(sum("Total Revenue").as("Total_Sales"))
    //find the country with highest total sales
    val topCountryDF=countrySalesDF.orderBy(desc("Total_Sales")).limit(1)
    topCountryDF.show()
    topCountryDF.write.parquet("C:/Users/shemantk/Desktop/requirement9.parquet")
    spark.stop()
  }

}


