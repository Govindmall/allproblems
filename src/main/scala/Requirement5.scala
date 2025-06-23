import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Requirement5{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1).cache()
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
