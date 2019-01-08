import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization


case class AeTokenId(org: String, tokenid: String, timestamp: String, score: Long, os: String)
case class CaptchaTokenId(org: String, tokenid: String, timestamp: String, os: String, step: String)

class tokenId extends Serializable{



   def ReadFileAe(spark: SparkSession, ae: RDD[Ae]): RDD[((String, String),(String, Int))] = {
     val res = ae.map(x=>(x.org,x.tokenid,x.timestamp,x.score))
       .filter(x=> !Config.filterKey.contains(x._2))
    val d = res.map(x=>{
      if(x._4>=Config.TokenIdisBlcak)
        ((x._1,x._2),(x._3, 1))
      else
        ((x._1,x._2),(x._3,0))
    })
    d
  }

   def ReadFileCaptcha(spark: SparkSession, captcha: RDD[Captcha]): RDD[((String, String),(String, Int))] = {
     val res = captcha.map(x=>((x.org,x.tokenid),(x.timestamp,0)))
                      .filter(x=>(!Config.shumeiOrgs.contains(x._1._2)))
     res
  }

  def union(d: RDD[((String, String),(String, Int))], d2: RDD[((String, String),(String, Int))]): RDD[((String, String),(String, Int))] = {
    val res = d.union(d2)
    res
  }



   def seqOpTokenId(r: (String, Int), item: (String, Int)): (String, Int) = {
    if (r._1 < item._1) item else r
  }

   def getMaxtimestampRecordTokenId(data: RDD[((String, String),(String, Int))]): RDD[(String, String,String, Int)] = {
    var s = ("", 0)
    data.aggregateByKey(s)(seqOpTokenId, seqOpTokenId).map(x=>(x._1._1, x._1._2, x._2._1, x._2._2))
  }


}
