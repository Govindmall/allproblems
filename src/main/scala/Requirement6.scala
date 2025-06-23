import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object Requirement6{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/sdattake/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1).cache()
    val itemsPriceCostDF = salesDF1.select("Item Type", "Unit Price", "Unit Cost").orderBy(asc("Unit Price"), asc("Unit Cost"))

    // Show the result
    itemsPriceCostDF.show()
    itemsPriceCostDF.write.parquet("C:/Users/sdattake/Desktop/requirement6.parquet")
    spark.stop()
  }

}