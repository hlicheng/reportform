import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

/**
  * @param org
  * @param phone
  * @param timestamp
  * @param phoneblack
  * @param os
  */

case class AePhone(org: String, phone: String, timestamp: String, phoneblack: Int, os: String)

class phoneUtil extends Serializable {


  def ReadFileAe(spark: SparkSession, ae:RDD[Ae]): RDD[((String, String),(String, Int))] = {
    val res = ae.map(x=>((x.org,x.phone),(x.timestamp,x.phoneblack)))
      .filter(x=> !Config.filterKey.contains(x._1._2))
    res

  }

  def seqOp(s: (String, Int), item: (String, Int)): (String, Int) = {
    if (s._1 < item._1) item else s
  }

  def combOp(s1: (String, Int), s2: (String, Int)): (String, Int) = {
    if (s1._1 < s2._1) s2 else s1
  }

  def getAeMaxtimestampRecordPhone(data: RDD[((String, String),(String, Int))]): RDD[(String,String,String, Int)] = {
    var s = ("",0)
    data.aggregateByKey(s)(seqOp, combOp).map(x=>(x._1._1, x._1._2, x._2._1, x._2._2))
  }


}
