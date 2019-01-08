import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object filterMainlog {

  def getrecord(spark: SparkSession, path: String): RDD[String] = {
    val data = spark.sparkContext.textFile(path)
    val res = data.filter(x=> x.contains("reqUri=") && x.contains("cost=") && x.contains("\"organization\":") && x.contains("\"os\":"))
    res
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                              .master("local[*]")
                               .appName("test")
                               .getOrCreate()
    val file = "file:///d:/mainlog"
    getrecord(spark,file).coalesce(1).saveAsTextFile("file:///d:/main3")
    spark.close()
  }
}
