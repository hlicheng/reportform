import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization


case class AeIp(org: String, ip: String, timestamp: String, os: String, proxyip: Int, proxyCatchTime: Long)
case class CaptchaIp(org: String, ip: String, timestamp: String, step: String, os: String)
case class FpIp(org: String, ip: String, timestamp: String, os: String)
class ipUtil extends  Serializable {



  def ReadFileAe(spark: SparkSession, ae:RDD[Ae]): RDD[((String, String),(String, Int))] = {
    val res = ae.map(x=>(x.org,x.ip,x.timestamp,x.proxyip,x.proxyCatchTime))
      .filter(x=> !Config.filterKey.contains(x._2))
      .map(x=>{
        if(x._4==1 && (x._3).toLong-x._5<Config.IpisBlack)
          ((x._1, x._2), (x._3, 1))
        else
          ((x._1, x._2), (x._3, 0))
      })
    res
  }

  def ReadFileCaptcha(spark: SparkSession, captcha: RDD[Captcha]): RDD[((String, String),(String, Int))] = {
    val res = captcha.map(x=>((x.org,x.ip),(x.timestamp,0)))
      .filter(x=> !Config.filterKey.contains(x._1._2))
    res
  }

  def ReadFileFp(spark: SparkSession, fp:RDD[Fp]): RDD[((String, String),(String, Int))] = {
    val res = fp.map(x=>((x.org,x.ip),(x.timestamp+"000",0))).filter(x=> !Config.filterKey.contains(x._1._2))
    res
  }

  def union(d1: RDD[((String, String),(String, Int))], d2: RDD[((String, String),(String, Int))], d3: RDD[((String, String),(String, Int))])
  : RDD[((String, String),(String, Int))] = {
    val res = d1.union(d2).union(d3)
    res
  }

  /**
    * P：seqOp   combOp
    * ((org, ip),(timestamp, proxyip, proxyCatchTime)) => (org, ip, timestamp, proxyip, proxyCatchTime)
    */
  def seqOpIp(s: (String, Int), item: (String, Int)): (String, Int) = {
    if (s._1 < item._1) item else s
  }

  def getMaxtimestampRecordIp(data: RDD[((String, String),(String, Int))]): RDD[(String, String, String, Int)] = {
    var s = ("",0 )
    data.aggregateByKey(s)(seqOpIp, seqOpIp).map(x=>(x._1._1,x._1._2,x._2._1,x._2._2))
  }
}
