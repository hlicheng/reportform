import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import scala.util.parsing.json.JSON

object stability {

  def getrecord(spark: SparkSession, path: String): RDD[String] = {
    val data = spark.sparkContext.textFile(path)
    val res = data.filter(x=> x.contains("reqUri=") && x.contains("cost=") && x.contains("\"organization\":") && x.contains("\"os\":"))
    res
  }

  def getMap(spark: SparkSession, path: String):RDD[(String, Float)] = {
    val data = spark.sparkContext.textFile(path)

    val res = data.map(line=>{
      val uri = line.split("\t")(0)
      val timeout = line.split("\t")(1).toFloat
      (uri -> timeout)
    })
   res
  }

  def getCol(spark: SparkSession, data: RDD[String], map: Map[String, Float]):RDD[((String, String),(Float, Int,  Int, Int))] = {
    val res = data.map(x=>{
      implicit val formats = Serialization.formats(NoTypeHints)
      val s = x.split("\t")
      val uri = s(4).split("=")(1)
      val cost = s(s.length-1).split("=")(1).toFloat
      var jsonStr = ""
      var organization = ""
      if (s.length <= 8) {
        jsonStr = s(6).split("=")(1)
        organization = JSON.parseFull(jsonStr).get.asInstanceOf[Map[String, String]].get("organization").getOrElse("#")
      } else {
        organization = s(6).split("=")(1)
      }
      val uricnt = 1
      val istimeout = if(map.get(uri).getOrElse[Float](0) < cost) 1 else 0
      val timeoutcnt = 0

      ((uri, organization), (cost, istimeout, uricnt, timeoutcnt))
    })
    res
  }



  def CalculatePerOrg(data: RDD[((String, String),(Float, Int, Int, Int))]):RDD[(String, String, Float, Int, Int)] = {

    def seqOp(r: (Float, Int, Int), item: (Float, Int, Int, Int)): (Float, Int, Int) = {
      val costsum = r._1 + item._1
      val uricnt = r._2 + item._3
      val timeoutcnt = if(item._2 == 1) r._3+1 else r._3
      (costsum, uricnt, timeoutcnt)
    }

    def combOp(r: (Float, Int, Int), item: (Float, Int, Int)): (Float, Int, Int) = {
      val costsum = r._1 + item._1
      val uricnt = r._2 + item._2
      val timeoutcnt = r._3 + item._3
      (costsum, uricnt, timeoutcnt)
    }
    val start:(Float, Int, Int) = (0, 0, 0)
    data.aggregateByKey(start)(seqOp, combOp).map(x=>(x._1._1, x._1._2, (x._2._1.toFloat)/(x._2._2.toFloat),x._2._2,x._2._3))
  }

  /**
    * uri  org  cost  istimeout  uricnt  timeoutcnt
    * @param data
    * @return
    */
  def CalculateAllOrg(data: RDD[((String, String),(Float, Int, Int, Int))]):RDD[(String, String, Float, Int, Int)] = {
    def seqOp(r: (Float, Int, Int), item: (String, Float, Int, Int, Int)): (Float, Int, Int) = {
      val costsum = r._1 + item._2
      val uricnt = r._2 + item._4
      val timeoutcnt = if(item._3 == 1) r._3 + 1 else r._3
      (costsum, uricnt, timeoutcnt)
    }

    def combOp(r: (Float, Int, Int), item: (Float, Int, Int)): (Float, Int, Int) = {
      (r._1+item._1, r._2+item._2, r._3+item._3)
    }
    val d = data.map(x=>(x._1._1, ("all",x._2._1,x._2._2,x._2._3,x._2._4)))
    val start: (Float, Int, Int) = (0, 0, 0)
    val res = d.aggregateByKey(start)(seqOp, combOp).map(x=>(x._1, "all", (x._2._1.toFloat)/(x._2._2.toFloat), x._2._2, x._2._3))
    res
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                             .master("local[*]")
                              .appName("test")
                               .getOrCreate()
    import spark.implicits._
    val log = "file:///d:/log"
    val uritime = "./src/main/stabilityIndex/uri"
    val map = getMap(spark, uritime).collect().toMap
    val data = getrecord(spark, log)
    val aimCol = getCol(spark, data, map)
    val perorg = CalculatePerOrg(aimCol).toDF("URI","organization","avg_time_consuming", "event_cnt", "overtime_cnt")
    val allorg = CalculateAllOrg(aimCol).toDF("URI","organization","avg_time_consuming", "event_cnt", "overtime_cnt")
    val res = perorg.union(allorg)
    res.coalesce(1).write.option("header", "true").csv("file:///d:/result")

    spark.close()
  }
}
