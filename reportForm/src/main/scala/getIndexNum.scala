import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}

object getIndexNum {

  /**
    * org  i_smid_cnt_1d, i_android_cnt_1d, i_ios_cnt_1d, i_ip_cnt_1d, i_phone_cnt_1d, i_tokenid_cnt_1d
    * @param spark
    * @param date
    * @return
    */
  def getOnedayPerOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidOneday(spark, date).createOrReplaceTempView("smid1")
    Calculate.getOneDay(spark, date, "reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ip1")
    Calculate.getOneDay(spark, date, "reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phone1")
    Calculate.getOneDay(spark, date, "reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("token1")
    val oneday_perOrg =
      s"""
        | select if(t2.organization is null,a4.organization,t2.organization) as organization,t2.i_smid_cnt_per_org_1d, t2.i_ip_cnt_per_org_1d, t2.i_phone_cnt_per_org_1d,t2.i_android_cnt_per_org_1d,t2.i_ios_cnt_per_org_1d, a4.i_tokenid_cnt_per_org_1d
        | from(
        |   select if(t1.organization is null,a3.organization,t1.organization) as organization,t1.i_smid_cnt_per_org_1d, t1.i_ip_cnt_per_org_1d, a3.i_phone_cnt_per_org_1d,t1.i_android_cnt_per_org_1d,t1.i_ios_cnt_per_org_1d
        |   from(
        |     select if(a1.organization is null,a2.organization,a1.organization) as organization,a1.i_smid_cnt_per_org_1d, a2.i_ip_cnt_per_org_1d,a1.i_android_cnt_per_org_1d,a1.i_ios_cnt_per_org_1d
        |     from smid1 as a1 full join ip1 as a2 on a1.organization=a2.organization
        |     ) t1
        |   full join phone1 as a3 on t1.organization=a3.organization
        |   ) t2
        | full join token1 as a4 on t2.organization=a4.organization
      """.stripMargin
    spark.sql(oneday_perOrg)

  }

  /**
    * org  i_smid_cnt_7d, i_ip_cnt_7d, i_phone_cnt_7d, i_tokenid_cnt_7d
    * @param spark
    * @param date
    * @return
    */
  def getSevendayPerOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidSevenDays(spark, date).createOrReplaceTempView("smid2")
    Calculate.getSevenDays(spark, date, "reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ip2")
    Calculate.getSevenDays(spark, date, "reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phone2")
    Calculate.getSevenDays(spark, date, "reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("token2")
    val sevenay_perOrg =
      s"""
         | select if(t2.organization is null,a4.organization,t2.organization) as organization,t2.i_smid_cnt_per_org_7d,t2.i_android_cnt_per_org_7d,t2.i_ios_cnt_per_org_7d, t2.i_ip_cnt_per_org_7d, t2.i_phone_cnt_per_org_7d, a4.i_tokenid_cnt_per_org_7d
         | from(
         |   select if(t1.organization is null,a3.organization,t1.organization) as organization,t1.i_smid_cnt_per_org_7d,t1.i_android_cnt_per_org_7d,t1.i_ios_cnt_per_org_7d, t1.i_ip_cnt_per_org_7d, a3.i_phone_cnt_per_org_7d
         |   from(
         |     select if(a1.organization is null,a2.organization,a1.organization) as organization,a1.i_smid_cnt_per_org_7d,a1.i_android_cnt_per_org_7d,a1.i_ios_cnt_per_org_7d, a2.i_ip_cnt_per_org_7d
         |     from smid2 as a1 full join ip2 as a2 on a1.organization=a2.organization
         |     ) t1
         |   full join phone2 as a3 on t1.organization=a3.organization
         |   ) t2
         | full join token2 as a4 on t2.organization=a4.organization
      """.stripMargin
    spark.sql(sevenay_perOrg)

  }

  /**
    * org  i_smid_cnt_30d, i_ip_cnt_30d, i_phone_cnt_30d, i_tokenid_cnt_30d
    * @param spark
    * @param date
    * @return
    */
  def getThirtydayPerOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidThirtyDays(spark, date).createOrReplaceTempView("smid3")
    Calculate.getThirtyDays(spark, date, "reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ip3")
    Calculate.getThirtyDays(spark, date, "reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phone3")
    Calculate.getThirtyDays(spark, date, "reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("token3")
    val thirtyday_perOrg =
      s"""
         | select if(t2.organization is null,a4.organization,t2.organization) as organization,t2.i_smid_cnt_per_org_30d,t2.i_android_cnt_per_org_30d, t2.i_ios_cnt_per_org_30d, t2.i_ip_cnt_per_org_30d, t2.i_phone_cnt_per_org_30d, a4.i_tokenid_cnt_per_org_30d
         | from(
         |   select if(t1.organization is null,a3.organization,t1.organization) as organization,t1.i_smid_cnt_per_org_30d,t1.i_android_cnt_per_org_30d, t1.i_ios_cnt_per_org_30d, t1.i_ip_cnt_per_org_30d, a3.i_phone_cnt_per_org_30d
         |   from(
         |     select if(a1.organization is null,a2.organization,a1.organization) as organization,a1.i_smid_cnt_per_org_30d,a1.i_android_cnt_per_org_30d, a1.i_ios_cnt_per_org_30d,a2.i_ip_cnt_per_org_30d
         |     from smid3 as a1 full join ip3 as a2 on a1.organization=a2.organization
         |     ) t1
         |   full join phone3 as a3 on t1.organization=a3.organization
         |   ) t2
         | full join token3 as a4 on t2.organization=a4.organization
      """.stripMargin
    spark.sql(thirtyday_perOrg)

  }

  /**
    * org  i_smid_cnt_total, i_ip_cnt_total, i_phone_cnt_total, i_tokenid_cnt_total
    * @param spark
    * @param date
    * @return
    */
  def getTotaldayPerOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidTotalDays(spark, date).createOrReplaceTempView("smid4")
    Calculate.getTotal(spark, "reporting.ip","ip").createOrReplaceTempView("ip4")
    Calculate.getTotal(spark,  "reporting.phone","phone").createOrReplaceTempView("phone4")
    Calculate.getTotal(spark, "reporting.tokenid","tokenid").createOrReplaceTempView("token4")
    val total_perOrg =
      s"""
         | select if(t2.organization is null,a4.organization,t2.organization) as organization,t2.i_smid_cnt_per_org_total,t2.i_android_cnt_per_org_total, t2.i_ios_cnt_per_org_total, t2.i_ip_cnt_per_org_total, t2.i_phone_cnt_per_org_total, a4.i_tokenid_cnt_per_org_total
         | from(
         |   select if(t1.organization is null,a3.organization,t1.organization) as organization,t1.i_smid_cnt_per_org_total,t1.i_android_cnt_per_org_total, t1.i_ios_cnt_per_org_total, t1.i_ip_cnt_per_org_total, a3.i_phone_cnt_per_org_total
         |   from(
         |     select if(a1.organization is null,a2.organization,a1.organization) as organization,a1.i_smid_cnt_per_org_total,a1.i_android_cnt_per_org_total, a1.i_ios_cnt_per_org_total, a2.i_ip_cnt_per_org_total
         |     from smid4 as a1 full join ip4 as a2 on a1.organization=a2.organization
         |     ) t1
         |   full join phone4 as a3 on t1.organization=a3.organization
         |   ) t2
         | full join token4 as a4 on t2.organization=a4.organization
      """.stripMargin
    spark.sql(total_perOrg)

  }

  /**
    * org  i_smid_cnt_1d, i_ip_cnt_1d, i_phone_cnt_1d, i_tokenid_cnt_1d, i_smid_cnt_7d, i_ip_cnt_7d, i_phone_cnt_7d, i_tokenid_cnt_7d
    *      i_smid_cnt_30d, i_ip_cnt_30d, i_phone_cnt_30d, i_tokenid_cnt_30d, i_smid_cnt_total, i_ip_cnt_total, i_phone_cnt_total, i_tokenid_cnt_total
    * @param spark
    * @param date
    * @return
    */
  def getIndexPerOrg(spark: SparkSession, date: String): DataFrame = {
     getOnedayPerOrg(spark, date).createOrReplaceTempView("onedayp")
     getSevendayPerOrg(spark, date).createOrReplaceTempView("sevendaysp")
     getThirtydayPerOrg(spark, date).createOrReplaceTempView("thirtydaysp")
     getTotaldayPerOrg(spark, date).createOrReplaceTempView("totalp")
    val index =
      s"""
        | select if(t2.organization is null,totalp.organization,t2.organization) as organization,t2.i_smid_cnt_per_org_1d,t2.i_ip_cnt_per_org_1d,t2.i_phone_cnt_per_org_1d,t2.i_android_cnt_per_org_1d, t2.i_ios_cnt_per_org_1d,t2.i_tokenid_cnt_per_org_1d,
        |          t2.i_ios_cnt_per_org_7d,t2.i_android_cnt_per_org_7d, t2.i_smid_cnt_per_org_7d, t2.i_ip_cnt_per_org_7d,t2.i_phone_cnt_per_org_7d,t2.i_tokenid_cnt_per_org_7d,
        |          t2.i_ios_cnt_per_org_30d,t2.i_android_cnt_per_org_30d, t2.i_smid_cnt_per_org_30d, t2.i_ip_cnt_per_org_30d,t2.i_phone_cnt_per_org_30d,t2.i_tokenid_cnt_per_org_30d,
        | totalp.i_ios_cnt_per_org_total,totalp.i_android_cnt_per_org_total,totalp.i_smid_cnt_per_org_total,totalp.i_ip_cnt_per_org_total,totalp.i_phone_cnt_per_org_total,totalp.i_tokenid_cnt_per_org_total,${date} as date
        | from
        | (select if(t1.organization is null,thirtydaysp.organization,t1.organization) as organization, t1.i_smid_cnt_per_org_1d,t1.i_ip_cnt_per_org_1d,t1.i_phone_cnt_per_org_1d,t1.i_android_cnt_per_org_1d, t1.i_ios_cnt_per_org_1d,t1.i_tokenid_cnt_per_org_1d,
        |          t1.i_android_cnt_per_org_7d, t1.i_ios_cnt_per_org_7d,t1.i_smid_cnt_per_org_7d, t1.i_ip_cnt_per_org_7d,t1.i_phone_cnt_per_org_7d,t1.i_tokenid_cnt_per_org_7d,
        |          thirtydaysp.i_android_cnt_per_org_30d, thirtydaysp.i_ios_cnt_per_org_30d,thirtydaysp.i_smid_cnt_per_org_30d, thirtydaysp.i_ip_cnt_per_org_30d,thirtydaysp.i_phone_cnt_per_org_30d,thirtydaysp.i_tokenid_cnt_per_org_30d
        | from
        | (select if(onedayp.organization is null,sevendaysp.organization,onedayp.organization) as organization, onedayp.i_smid_cnt_per_org_1d,onedayp.i_ip_cnt_per_org_1d,onedayp.i_phone_cnt_per_org_1d,onedayp.i_android_cnt_per_org_1d, onedayp.i_ios_cnt_per_org_1d,onedayp.i_tokenid_cnt_per_org_1d,
        |          sevendaysp.i_smid_cnt_per_org_7d,sevendaysp.i_android_cnt_per_org_7d, sevendaysp.i_ios_cnt_per_org_7d, sevendaysp.i_ip_cnt_per_org_7d,sevendaysp.i_phone_cnt_per_org_7d,sevendaysp.i_tokenid_cnt_per_org_7d
        | from onedayp full join sevendaysp on onedayp.organization=sevendaysp.organization) t1
        | full join thirtydaysp on t1.organization=thirtydaysp.organization) t2
        | full join totalp on t2.organization=totalp.organization
      """.stripMargin
    spark.sql(index)
  }

  /**
    * org=all i_smid_cnt_1d, i_ip_cnt_1d, i_phone_cnt_1d, i_tokenid_cnt_1d
    * ID info of all orgs
    * @param spark
    * @param date
    * @return
    */

  def getOnedayAllOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidOnedayAllOrg(spark, date).createOrReplaceTempView("all_smid")
    Calculate.getOneDayAllOrg(spark, date, "reporting.ip", "ip", "i_ip_last_active_time").createOrReplaceTempView("all_ip")
    Calculate.getOneDayAllOrg(spark, date, "reporting.phone", "phone", "i_phone_last_active_time").createOrReplaceTempView("all_phone")
    Calculate.getOneDayAllOrg(spark, date, "reporting.tokenid", "tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("all_tokenid")
    spark.sql(
      """
        | select t2.organization,t2.i_smid_cnt_per_org_1d,t2.i_ip_cnt_per_org_1d,t2.i_phone_cnt_per_org_1d,t2.i_android_cnt_per_org_1d,t2.i_ios_cnt_per_org_1d,all_tokenid.i_tokenid_cnt_per_org_1d
        | from
        | (select t1.*, all_phone.i_phone_cnt_per_org_1d
        | from
        | (select all_smid.*, all_ip.i_ip_cnt_per_org_1d from all_smid full join all_ip on all_smid.organization=all_ip.organization) t1
        | full join all_phone on t1.organization=all_phone.organization) t2
        | full join all_tokenid on t2.organization=all_tokenid.organization
      """.stripMargin)
  }

  /**
    * org=all i_smid_cnt_7d, i_ip_cnt_7d, i_phone_cnt_7d, i_tokenid_cnt_7d
    * @param spark
    * @param date
    * @return
    */
  def getSevendayAllOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidSevendayAllOrg(spark, date).createOrReplaceTempView("all_smid2")
    Calculate.getSevenDayAllOrg(spark, date, "reporting.ip", "ip", "i_ip_last_active_time").createOrReplaceTempView("all_ip2")
    Calculate.getSevenDayAllOrg(spark, date, "reporting.phone", "phone", "i_phone_last_active_time").createOrReplaceTempView("all_phone2")
    Calculate.getSevenDayAllOrg(spark, date, "reporting.tokenid", "tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("all_tokenid2")
    spark.sql(
      """
        | select t2.organization,t2.i_ios_cnt_per_org_7d,t2.i_android_cnt_per_org_7d,t2.i_smid_cnt_per_org_7d,t2.i_ip_cnt_per_org_7d,t2.i_phone_cnt_per_org_7d,all_tokenid2.i_tokenid_cnt_per_org_7d
        | from
        | (select t1.*, all_phone2.i_phone_cnt_per_org_7d
        | from
        | (select all_smid2.*, all_ip2.i_ip_cnt_per_org_7d from all_smid2 full join all_ip2 on all_smid2.organization=all_ip2.organization) t1
        | full join all_phone2 on t1.organization=all_phone2.organization) t2
        | full join all_tokenid2 on t2.organization=all_tokenid2.organization
      """.stripMargin)
  }

  /**
    * org=all i_smid_cnt_30d, i_ip_cnt_30d, i_phone_cnt_30d, i_tokenid_cnt_30d
    * @param spark
    * @param date
    * @return
    */
  def getThirtydayAllOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidThirtyAllOrg(spark, date).createOrReplaceTempView("all_smid3")
    Calculate.getThirtyDayAllOrg(spark, date, "reporting.ip", "ip", "i_ip_last_active_time").createOrReplaceTempView("all_ip3")
    Calculate.getThirtyDayAllOrg(spark, date, "reporting.phone", "phone", "i_phone_last_active_time").createOrReplaceTempView("all_phone3")
    Calculate.getThirtyDayAllOrg(spark, date, "reporting.tokenid", "tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("all_tokenid3")
    spark.sql(
      """
        | select t2.organization,t2.i_ios_cnt_per_org_30d,t2.i_android_cnt_per_org_30d,t2.i_smid_cnt_per_org_30d,t2.i_ip_cnt_per_org_30d,t2.i_phone_cnt_per_org_30d,all_tokenid3.i_tokenid_cnt_per_org_30d
        | from
        | (select t1.*, all_phone3.i_phone_cnt_per_org_30d
        | from
        | (select all_smid3.*, all_ip3.i_ip_cnt_per_org_30d from all_smid3 full join all_ip3 on all_smid3.organization=all_ip3.organization) t1
        | full join all_phone3 on t1.organization=all_phone3.organization) t2
        | full join all_tokenid3 on t2.organization=all_tokenid3.organization
      """.stripMargin)
  }

  /**
    * org=all i_smid_cnt_total, i_ip_cnt_total, i_phone_cnt_total, i_tokenid_cnt_total
    * @param spark
    * @param date
    * @return
    */
  def getTotalAllOrg(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSmidTotalAllOrg(spark, date).createOrReplaceTempView("all_smid4")
    Calculate.getTotalAllOrg(spark, date, "reporting.ip", "ip").createOrReplaceTempView("all_ip4")
    Calculate.getTotalAllOrg(spark, date, "reporting.phone", "phone").createOrReplaceTempView("all_phone4")
    Calculate.getTotalAllOrg(spark, date, "reporting.tokenid", "tokenid").createOrReplaceTempView("all_tokenid4")
    spark.sql(
      """
        | select t2.organization,t2.i_ios_cnt_per_org_total,t2.i_android_cnt_per_org_total,t2.i_smid_cnt_per_org_total,t2.i_ip_cnt_per_org_total,t2.i_phone_cnt_per_org_total, all_tokenid4.i_tokenid_cnt_per_org_total
        | from
        | (select t1.*, all_phone4.i_phone_cnt_per_org_total
        | from
        | (select all_smid4.*, all_ip4.i_ip_cnt_per_org_total from all_smid4 full join all_ip4 on all_smid4.organization=all_ip4.organization) t1
        | full join all_phone4 on t1.organization=all_phone4.organization) t2
        | full join all_tokenid4 on t2.organization=all_tokenid4.organization
      """.stripMargin)
  }

  /**
    * only one record!!!!!!!!!!!!!!!!
    * @param spark
    * @param date
    * @return
    */
  def getIndexAllOrg(spark: SparkSession, date: String): DataFrame = {
    getOnedayAllOrg(spark, date).createOrReplaceTempView("onedayallorg")
    getSevendayAllOrg(spark, date).createOrReplaceTempView("sevendayallorg")
    getThirtydayAllOrg(spark, date).createOrReplaceTempView("thirtydayallorg")
    getTotalAllOrg(spark, date).createOrReplaceTempView("totalallorg")
    val indexallorg =
      s"""
         | select t2.*,totalallorg.i_ios_cnt_per_org_total,totalallorg.i_android_cnt_per_org_total,totalallorg.i_smid_cnt_per_org_total,totalallorg.i_ip_cnt_per_org_total,totalallorg.i_phone_cnt_per_org_total,totalallorg.i_tokenid_cnt_per_org_total,${date} as date
         | from
         | (select t1.*, thirtydayallorg.i_ios_cnt_per_org_30d,thirtydayallorg.i_android_cnt_per_org_30d,thirtydayallorg.i_smid_cnt_per_org_30d, thirtydayallorg.i_ip_cnt_per_org_30d,thirtydayallorg.i_phone_cnt_per_org_30d,thirtydayallorg.i_tokenid_cnt_per_org_30d
         | from
         | (select onedayallorg.*, sevendayallorg.i_ios_cnt_per_org_7d,sevendayallorg.i_android_cnt_per_org_7d,sevendayallorg.i_smid_cnt_per_org_7d, sevendayallorg.i_ip_cnt_per_org_7d,sevendayallorg.i_phone_cnt_per_org_7d,sevendayallorg.i_tokenid_cnt_per_org_7d
         | from onedayallorg full join sevendayallorg on onedayallorg.organization=sevendayallorg.organization) t1
         | full join thirtydayallorg on t1.organization=thirtydayallorg.organization) t2
         | full join totalallorg on t2.organization=totalallorg.organization
      """.stripMargin
    spark.sql(indexallorg)
  }

  /**
    * make perorg join allorg(only one record)
    * @param spark
    * @param date
    * @return
    */
  def getSumtable(spark: SparkSession, date: String): DataFrame = {
    val perorg = getIndexPerOrg(spark, date)
    val allorg = getIndexAllOrg(spark, date)
    perorg.union(allorg)
  }

}
