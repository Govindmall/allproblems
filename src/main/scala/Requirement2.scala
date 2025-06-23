import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, sum}

object Requirement2{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/sakmahe/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1).cache()
    val unitsSoldByRegionDF = salesDF1.groupBy("Region").agg(sum("Units Sold").as("Total_Units_Sold"))
      .orderBy(desc("Total_Units_Sold"))
    unitsSoldByRegionDF.show()
    unitsSoldByRegionDF.write.parquet("C:/Users/sakmahe/Desktop/requirement2.parquet")
    spark.stop()
  }

}
