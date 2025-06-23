import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, to_date}

object Requirement3{
  def main(args: Array[String]): Unit = {
    //Spark Session created
    val spark = SparkSession.builder().appName("DistinctCountries").master("local[*]").getOrCreate()
    //Dataset(csv) file into DF
    val DateDF = spark.read.option("header", "true").csv("C:/Users/pratdeep/Downloads/sparsales.csv")
    val DateDF1=DateDF.coalesce(1)
    // Order Date to DateType(optimization)
    val dfWithDate = DateDF1.withColumn("Order Date", to_date(col("Order Date"), "M/d/yyyy"))
    // orderby Order Date desc and take top 10
    val recentSales = dfWithDate.orderBy(col("Order Date").desc).limit(10)
    recentSales.show()
    recentSales.write.mode("overwrite").parquet("C:/Results/SprintOPQ3.parquet")
    recentSales.write.mode("overwrite").csv("C:/Results/SprintOPQ3csv.csv")
    spark.stop()
  }
}
