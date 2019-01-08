import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MainFunc {
    def main(args: Array[String]): Unit = {
      val date = args(0)
      dateUtil.now = date
      dateUtil.onedayago = dateUtil.getDate(date,1)

      val spark = SparkSession.builder()
        .master("yarn")
        .appName("reportForm")
        .enableHiveSupport()
        .getOrCreate()

      val ae = ReadFile.getAllColOfAe(spark, Config.aePath.format(date))
      ae.persist(StorageLevel.DISK_ONLY)
      val captcha = ReadFile.getAllColOfCaptcha(spark, Config.captchaPath.format(date))
      captcha.persist(StorageLevel.DISK_ONLY)
      val fp = ReadFile.getAllColOfFp(spark, Config.fpPath.format(date))
      fp.persist(StorageLevel.DISK_ONLY)

      Operation.AeJoinCaptchaJoinFpSmid(spark,ae,captcha,fp)
      Operation.AeJoinCaptchaTokenId(spark,ae,captcha)
      Operation.AeJoinCaptchaJoinFpIp(spark,ae,captcha,fp)
      Operation.Phone(spark, ae)

      import spark.implicits._
      getIndexNum.getSumtable(spark, date).coalesce(1).write.csv(s"/user/data/reporting/reportform_id/id${date}")
      getblackIndex.getSumOrgBlack(spark, date).coalesce(1).write.csv(s"/user/data/reporting/reportform_id/blackid${date}")
      spark.close()
  }
}
