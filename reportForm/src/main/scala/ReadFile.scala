import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

case class Ae(org: String, tokenid: String, score:Long, smid: String, ip: String, proxyip:Int, proxyCatchTime: Long,
              phone: String, phoneblack:Int, timestamp: String, os: String)
case class Captcha(org: String, tokenid: String, smid: String, ip: String, timestamp: String, os: String, step: String)
case class Fp(org: String, smid: String, ip: String, timestamp: String, os: String)


object ReadFile {

  def getAllColOfAe(spark: SparkSession, ae: String):RDD[Ae] = {
    val data = spark.sparkContext.textFile(ae)
    val res = data.map(line=>{
      implicit val formats = Serialization.formats(NoTypeHints)
      val l = parse(line)
      Ae (
        org = (l \ "organization").extractOpt[String].getOrElse("#"),
        tokenid = (l \ "data" \ "tokenId").extractOpt[String].getOrElse("#"),
        score = (l \ "features" \ "profile.token_score_v1").extractOpt[Long].getOrElse(-1),
        smid = (l \ "features" \ "profile.smid").extractOpt[String].getOrElse("#"),
        ip = (l \ "data" \ "ip").extractOpt[String].getOrElse("#"),
        proxyip = (l \ "features" \ "profile.proxyip").extractOpt[Int].getOrElse(-1),
        proxyCatchTime = (l \ "features" \ "profile.proxyip_catch_time").extractOpt[Long].getOrElse(-1),
        phone = (l \ "features" \ "profile.phone").extractOpt[String].getOrElse("#"),
        phoneblack = (l \ "features" \ "profile.phone_i_histo_black").extractOpt[Int].getOrElse(0),
        timestamp = (l \ "timestamp").extractOpt[String].getOrElse("#"),
        os = (l \ "features" \ "profile.smid_os").extractOpt[String].getOrElse("#")
      )
    }).filter(x=>(!Config.filterKey.contains(x.org) && !Config.shumeiOrgs.contains(x.org)
                    && (Config.filterOs.contains(x.os)))).repartition(2000)
    res
  }


  def getAllColOfCaptcha(spark: SparkSession, captcha: String):RDD[Captcha] = {
    val data = spark.sparkContext.textFile(captcha)
    val res = data.map(line=>{
      implicit val formats = Serialization.formats(NoTypeHints)
      val l = parse(line)
      Captcha (
        org = (l \ "organization").extractOpt[String].getOrElse("#"),
        tokenid = (l \ "feature" \ "data.tokenId").extractOpt[String].getOrElse("#"),
        smid = (l \ "feature" \ "profile.smid").extractOpt[String].getOrElse("#"),
        ip = (l \ "feature" \ "data.ip").extractOpt[String].getOrElse("#"),
        timestamp = (l \ "timestamp").extractOpt[String].getOrElse("#"),
        os = (l \ "feature" \ "captcha.os").extractOpt[String].getOrElse("#"),
        step = (l \ "step" ).extractOpt[String].getOrElse("#")
      )
    }).filter(x=>(!Config.filterKey.contains(x.org) && !Config.shumeiOrgs.contains(x.org)
      && (x.step=="sverify" && Config.filterOs.contains(x.os))))
    res
  }

  def getAllColOfFp(spark: SparkSession, fp: String): RDD[Fp] = {
    val data = spark.sparkContext.textFile(fp)
    val res = data.map(line=>{
      implicit val formats = Serialization.formats(NoTypeHints)
      val l = parse(line)
      Fp (
        org = (l \ "org").extractOpt[String].getOrElse("#"),
        smid = (l \ "smid").extractOpt[String].getOrElse("#"),
        ip = (l \ "ip").extractOpt[String].getOrElse("#"),
        timestamp = (l \ "t").extractOpt[String].getOrElse("#"),
        os = (l \ "data" \ "os").extractOpt[String].getOrElse("#")
      )
    }).filter(x=>(!Config.filterKey.contains(x.org) && !Config.shumeiOrgs.contains(x.org)
      && Config.filterOs.contains(x.os)))
    res
  }

}
