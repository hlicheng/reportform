import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization


class smidUtil extends Serializable{
   def ReadFileAe(spark: SparkSession, ae:RDD[Ae]): RDD[((String, String),(String, String))] = {
     val res = ae.map(x=>((x.org,x.smid),(x.timestamp,x.os))).filter(x=> !Config.filterKey.contains(x._1._2))
    res
  }

   def ReadFileCaptcha(spark: SparkSession, captcha: RDD[Captcha]): RDD[((String, String),(String, String))] = {
     val res = captcha.map(x=>((x.org,x.smid),(x.timestamp,x.os))).filter(x=> !Config.filterKey.contains(x._1._2))
    res
  }

   def ReadFileFp(spark: SparkSession, fp:RDD[Fp]): RDD[((String, String),(String, String))] = {
     val res = fp.map(x=>((x.org,x.smid),(x.timestamp+"000", x.os))).filter(x=> !Config.filterKey.contains(x._1._2))
    res
  }

  def union(d1: RDD[((String, String),(String, String))], d2: RDD[((String, String),(String, String))], d3: RDD[((String, String),(String, String))]): RDD[((String, String),(String, String))] = {
    val res = d1.union(d2).union(d3)
    res
  }


  def seqOpSmid(r: (String, String), item: (String, String)): (String, String) = {
    if (r._1 < item._1) item else r
  }

  def getMaxtimestampRecordSmid(data: RDD[((String, String),(String, String))]): RDD[(String, String,String, String)] = {
    var s = ("","")
    data.aggregateByKey(s)(seqOpSmid, seqOpSmid).map(x=>(x._1._1, x._1._2, x._2._2,x._2._1))
  }

}
