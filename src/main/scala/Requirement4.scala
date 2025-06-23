import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, length, regexp_replace}

object Requirement4{
  def main(args: Array[String]): Unit = {
    // created Spark Session
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //read the csv file into DataFrame
    val salesDF = spark.read.option("header", "true").csv("C:/Users/gomall/Downloads/sparsales.csv")
    val salesDF1=salesDF.coalesce(1).cache()
    val productsWithTwoAs = salesDF1.select("Item Type").distinct()
      .filter(length(regexp_replace(col("Item Type"), "[^aA]", "")) >= 2)
    productsWithTwoAs.show()
    productsWithTwoAs.write.parquet("C:/Users/gomall/Desktop/requirement4.parquet")
    spark.stop()
  }

}
