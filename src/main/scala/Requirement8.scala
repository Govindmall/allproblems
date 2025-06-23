import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Requirement8{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/shemantk/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1)
    //group by item type and count the number of orders
    val itemOrdersDF=salesDF1.groupBy("Item Type").agg(count("Order ID").as("Number_of_orders"))
    itemOrdersDF.show()
    itemOrdersDF.write.partitionBy("Item Type").parquet("C:/Users/shemantk/Desktop/requirement8.parquet")
    spark.stop()
  }
}
