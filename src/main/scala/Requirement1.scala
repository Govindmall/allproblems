import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
object Requirement1{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/sakmahe/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1).cache()
    val numCountriesDF = salesDF1
      .agg(countDistinct("Country").as("Number_of_Countries"))
    numCountriesDF.show()
    numCountriesDF.write.parquet("C:/Users/sakmahe/Desktop/requirement1.parquet")
    spark.stop()
  }
}
