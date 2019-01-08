import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object Operation {



  /**
    * TokenId  "organization", "tokenId", "timestamp", "isblack"
    */
  def AeJoinCaptchaTokenId(spark: SparkSession, ae:RDD[Ae], captcha: RDD[Captcha]): Unit = {
    import spark.implicits._
    val tokenid = new tokenId()
    val aerdd = tokenid.ReadFileAe(spark, ae)
    val captchardd = tokenid.ReadFileCaptcha(spark, captcha)
    val finalrdd = tokenid.union(aerdd, captchardd)
    tokenid.getMaxtimestampRecordTokenId(finalrdd)
      .toDF("organization", "tokenid", "i_tokenid_last_active_time", "is_black").createOrReplaceTempView("tokenid_now")
    spark.sql(Config.tokenIdSql1)
    spark.sql(Config.tokenIdSql2.format(dateUtil.onedayago))
    spark.sql(Config.tokenIdSql3.format(dateUtil.now))
//    spark.sql(Config.tokenIdSql4)
//    spark.sql(Config.tokenIdSql5)
  }


  def AeJoinCaptchaJoinFpSmid(spark: SparkSession, ae: RDD[Ae], captcha: RDD[Captcha], fp:RDD[Fp]): Unit = {
    import spark.implicits._
    val smid = new smidUtil()
    val aerdd = smid.ReadFileAe(spark, ae)
    val captchardd = smid.ReadFileCaptcha(spark, captcha)
    val fprdd = smid.ReadFileFp(spark, fp)
    val finalrdd = smid.union(aerdd, captchardd, fprdd)
    smid.getMaxtimestampRecordSmid(finalrdd)
      .toDF("organization", "smid", "smid_os", "i_smid_last_active_time").createOrReplaceTempView("smid_now")
    spark.sql(Config.smidSql1)
    spark.sql(Config.smidSql2.format(dateUtil.onedayago))
    spark.sql(Config.smidSql3.format(dateUtil.now))
//    spark.sql(Config.smidSql4)
//    spark.sql(Config.smidSql5)

  }

  /**
    * Ip: "organization", "smid", "timestamp", "isblack"
    *
    */
  def AeJoinCaptchaJoinFpIp(spark: SparkSession, ae:RDD[Ae], captcha: RDD[Captcha], fp:RDD[Fp]): Unit = {
    import spark.implicits._
    val ip = new ipUtil()
    val aerdd = ip.ReadFileAe(spark, ae)
    val captchardd = ip.ReadFileCaptcha(spark, captcha)
    val fprdd = ip.ReadFileFp(spark, fp)
    val finalrdd = ip.union(aerdd, captchardd, fprdd)
    ip.getMaxtimestampRecordIp(finalrdd).toDF("organization", "ip", "i_ip_last_active_time", "is_black")
      .createOrReplaceTempView("ip_now")
    spark.sql(Config.ipSql1)
    spark.sql(Config.ipSql2.format(dateUtil.onedayago))
    spark.sql(Config.ipSql3.format(dateUtil.now))
//    spark.sql(Config.ipSql4)
//    spark.sql(Config.ipSql5)

  }

  /**
    * "organization", "phone", "timestamp", "phoneblack"
    */

  def Phone(spark: SparkSession, ae:RDD[Ae]) : Unit = {
    import spark.implicits._
    val phone = new phoneUtil()
    val p = phone.getAeMaxtimestampRecordPhone(phone.ReadFileAe(spark, ae))
    .toDF("organization", "phone", "i_phone_last_active_time", "is_black").createOrReplaceTempView("phone_now")
    spark.sql(Config.phoneSql1)
    spark.sql(Config.phoneSql2.format(dateUtil.onedayago))
    spark.sql(Config.phoneSql3.format(dateUtil.now))
//    spark.sql(Config.phoneSql4)
//    spark.sql(Config.phoneSql5)
  }
}

