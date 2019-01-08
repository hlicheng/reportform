import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}

object Calculate {
  def getOneDay(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val OneAgoTimestampS = d.getTime
    val OneAgoTimestampE = d.getTime + Config.OneDay

    val oneday_perOrg =
          s"""
              | select t2.organization,if(i_${col1}_cnt_per_org_1d is null, 0, i_${col1}_cnt_per_org_1d) as i_${col1}_cnt_per_org_1d
              | from
              | (select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_1d
              | from
              | (select organization, ${col1}
              |  from ${tablename} where bigint(${col2})>=${OneAgoTimestampS} ) t1
              |  group by t1.organization) t2
           """.stripMargin

    spark.sql(oneday_perOrg)
    }

  def getSmidOneday(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val OneAgoTimestampS = d.getTime
    val OneAgoTimestampE = d.getTime + Config.OneDay
    spark.sql(s"select organization, smid, smid_os from reporting.smid where bigint(i_smid_last_active_time)>=${OneAgoTimestampS} ").createOrReplaceTempView("smidoneday")
    spark.sql(s"select organization, count(distinct(smid)) as i_smid_cnt_per_org_1d from smidoneday group by organization").createOrReplaceTempView("smidoneall")
    spark.sql("select t2.organization, if(t2.i_android_cnt_per_org_1d is null, 0, i_android_cnt_per_org_1d) as i_android_cnt_per_org_1d from (select organization, count(distinct(smid)) as i_android_cnt_per_org_1d from (select organization, smid from smidoneday where smid_os='android') t1 group by organization) t2 ").createOrReplaceTempView("androidone")
    spark.sql("select t2.organization, if(t2.i_ios_cnt_per_org_1d is null, 0, i_ios_cnt_per_org_1d) as i_ios_cnt_per_org_1d from (select organization, count(distinct(smid)) as i_ios_cnt_per_org_1d from (select organization, smid from smidoneday where smid_os='ios') t1 group by organization) t2 ").createOrReplaceTempView("iosone")
    spark.sql(
      """
        | select t.*, smidoneall.i_smid_cnt_per_org_1d
        | from
        | (select if(androidone.organization is null,iosone.organization,androidone.organization) as organization,androidone.i_android_cnt_per_org_1d, iosone.i_ios_cnt_per_org_1d
        | from androidone full join iosone on androidone.organization=iosone.organization) t
        | full join smidoneall on t.organization=smidoneall.organization
      """.stripMargin)
  }

  def getSevenDays(spark: SparkSession, date: String, tablename: String,col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val sevenAgoTimestampE = d.getTime + Config.OneDay
    val sevenAgoTimestampS = sevenAgoTimestampE - Config.SevenDays
    val sevenday_perOrg =
      s"""
         | select t2.organization,if(i_${col1}_cnt_per_org_7d is null, 0, i_${col1}_cnt_per_org_7d) as i_${col1}_cnt_per_org_7d
         | from
         | (select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_7d
         | from
         | (select organization, ${col1}
         |  from ${tablename} where bigint(${col2})>=${sevenAgoTimestampS} ) t1
         |  group by t1.organization) t2
      """.stripMargin
    spark.sql(sevenday_perOrg)
  }

  def getSmidSevenDays(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val sevenAgoTimestampE = d.getTime + Config.OneDay
    val sevenAgoTimestampS = sevenAgoTimestampE - Config.SevenDays
    spark.sql(s"select organization, smid, smid_os from reporting.smid where bigint(i_smid_last_active_time)>=${sevenAgoTimestampS} ").createOrReplaceTempView("smidsevenday")
    spark.sql(s"select organization, count(distinct(smid)) as i_smid_cnt_per_org_7d from smidsevenday group by organization").createOrReplaceTempView("smidsevenall")
    spark.sql("select t2.organization, if(t2.i_android_cnt_per_org_7d is null, 0, i_android_cnt_per_org_7d) as i_android_cnt_per_org_7d from (select organization, count(distinct(smid)) as i_android_cnt_per_org_7d from (select organization, smid from smidsevenday where smid_os='android') t1 group by organization) t2 ").createOrReplaceTempView("androidseven")
    spark.sql("select t2.organization, if(t2.i_ios_cnt_per_org_7d is null, 0, i_ios_cnt_per_org_7d) as i_ios_cnt_per_org_7d from (select organization, count(distinct(smid)) as i_ios_cnt_per_org_7d from (select organization, smid from smidsevenday where smid_os='ios') t1 group by organization) t2 ").createOrReplaceTempView("iosseven")
    spark.sql(
      """
        | select t.*, smidsevenall.i_smid_cnt_per_org_7d
        | from
        | (select if(androidseven.organization is null,iosseven.organization,androidseven.organization) as organization,androidseven.i_android_cnt_per_org_7d, iosseven.i_ios_cnt_per_org_7d
        | from androidseven full join iosseven on androidseven.organization=iosseven.organization) t
        | full join smidsevenall on t.organization=smidsevenall.organization
      """.stripMargin)
  }

  def getThirtyDays(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ThirtyAgoTimestampE = d.getTime + Config.OneDay
    val ThirtyAgoTimestampS = ThirtyAgoTimestampE - Config.ThirtyDays
    val thirtyday_perOrg =
      s"""
         | select t2.organization,if(i_${col1}_cnt_per_org_30d is null, 0, i_${col1}_cnt_per_org_30d) as i_${col1}_cnt_per_org_30d
         | from
         | (select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_30d
         | from
         | (select organization, ${col1}
         |  from ${tablename} where bigint(${col2})>=${ThirtyAgoTimestampS} ) t1
         |  group by t1.organization) t2
      """.stripMargin
    spark.sql(thirtyday_perOrg)
  }

  def getSmidThirtyDays(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ThirtyAgoTimestampE = d.getTime + Config.OneDay
    val ThirtyAgoTimestampS = ThirtyAgoTimestampE - Config.ThirtyDays
    spark.sql(s"select organization, smid, smid_os from reporting.smid where bigint(i_smid_last_active_time)>=${ThirtyAgoTimestampS}").createOrReplaceTempView("smidthirty")
    spark.sql(s"select organization, count(distinct(smid)) as i_smid_cnt_per_org_30d from smidthirty group by organization").createOrReplaceTempView("smidthiryall")
    spark.sql("select t2.organization, if(t2.i_android_cnt_per_org_30d is null, 0, i_android_cnt_per_org_30d) as i_android_cnt_per_org_30d from (select organization, count(distinct(smid)) as i_android_cnt_per_org_30d from (select organization, smid from smidthirty where smid_os='android') t1 group by organization) t2 ").createOrReplaceTempView("androidthirty")
    spark.sql("select t2.organization, if(t2.i_ios_cnt_per_org_30d is null, 0, i_ios_cnt_per_org_30d) as i_ios_cnt_per_org_30d from (select organization, if(count(distinct(smid))=0 or count(distinct(smid)) is null, 0, count(distinct(smid))) as i_ios_cnt_per_org_30d from (select organization, smid from smidthirty where smid_os='ios') t1 group by organization) t2 ").createOrReplaceTempView("iosthirty")
    spark.sql(
      """
        | select t.*, smidthiryall.i_smid_cnt_per_org_30d
        | from
        | (select if(androidthirty.organization is null,iosthirty.organization,androidthirty.organization) as organization,androidthirty.i_android_cnt_per_org_30d, iosthirty.i_ios_cnt_per_org_30d
        | from androidthirty full join iosthirty on androidthirty.organization=iosthirty.organization) t
        | full join smidthiryall on t.organization=smidthiryall.organization
      """.stripMargin)
  }

  def getTotal(spark: SparkSession, tablename: String, col: String): DataFrame = {
    val total_perOrg =
      s"""
         | select t2.organization, if(t2.i_${col}_cnt_per_org_total is null, 0, i_${col}_cnt_per_org_total) as i_${col}_cnt_per_org_total
         | from
         | (select organization, count(distinct(${col})) as i_${col}_cnt_per_org_total
         | from ${tablename} group by organization) t2
      """.stripMargin
    spark.sql(total_perOrg)
  }

  def getSmidTotalDays(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    spark.sql(s"select organization, count(distinct(smid)) as i_smid_cnt_per_org_total from reporting.smid group by organization").createOrReplaceTempView("smidtotalall")
    spark.sql("select t2.organization, if(t2.i_android_cnt_per_org_total is null, 0, i_android_cnt_per_org_total) as i_android_cnt_per_org_total from (select organization, count(distinct(smid)) as i_android_cnt_per_org_total from (select organization, smid from reporting.smid where smid_os='android') as t1 group by organization) t2 ").createOrReplaceTempView("androidtotal")
    spark.sql("select t2.organization, if(t2.i_ios_cnt_per_org_total is null, 0, i_ios_cnt_per_org_total) as i_ios_cnt_per_org_total from (select organization, count(distinct(smid)) as i_ios_cnt_per_org_total from (select organization, smid from reporting.smid where smid_os='ios') as t1 group by organization) t2 ").createOrReplaceTempView("iostotal")
    spark.sql(
      """
        | select t.*, smidtotalall.i_smid_cnt_per_org_total
        | from
        | (select if(androidtotal.organization is null,iostotal.organization,androidtotal.organization) as organization,androidtotal.i_android_cnt_per_org_total, iostotal.i_ios_cnt_per_org_total
        | from androidtotal full join iostotal on androidtotal.organization=iostotal.organization) t
        | full join smidtotalall on smidtotalall.organization=t.organization
      """.stripMargin)
  }


  def getOneDayAllOrg(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val OneAgoTimestampS = d.getTime
    val OneAgoTimestampE = d.getTime + Config.OneDay

    val oneday_perOrg =
      s"""
         | select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_1d
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where bigint(${col2})>=${OneAgoTimestampS} ) t1
         | group by t1.organization
       """.stripMargin
    spark.sql(oneday_perOrg)
  }

  def getSmidOnedayAllOrg(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val OneAgoTimestampS = d.getTime
    val OneAgoTimestampE = d.getTime + Config.OneDay
    spark.sql(
      s"""
        | select "all" as organization, smid, smid_os from reporting.smid where bigint(i_smid_last_active_time)>=${OneAgoTimestampS}
      """.stripMargin).createOrReplaceTempView("smidoneallorg")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_smid_cnt_per_org_1d from smidoneallorg").createOrReplaceTempView("allonesmid")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_android_cnt_per_org_1d from (select 'all' as organization, smid from smidoneallorg where smid_os='android') t1 group by organization").createOrReplaceTempView("adone")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_ios_cnt_per_org_1d from (select 'all' as organization, smid from smidoneallorg where smid_os='ios') t1 group by organization").createOrReplaceTempView("iosone")
    spark.sql(
      """
        | select t1.organization, allonesmid.i_smid_cnt_per_org_1d, t1.i_android_cnt_per_org_1d, t1.i_ios_cnt_per_org_1d
        | from
        | (select adone.organization, adone.i_android_cnt_per_org_1d, iosone.i_ios_cnt_per_org_1d
        | from adone full join iosone on adone.organization=iosone.organization) t1
        | full join allonesmid on allonesmid.organization=t1.organization
      """.stripMargin)
  }

  def getSevenDayAllOrg(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val sevenAgoTimestampE = d.getTime + Config.OneDay
    val sevenAgoTimestampS = sevenAgoTimestampE - Config.SevenDays

    val sevenday_perOrg =
      s"""
         | select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_7d
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where bigint(${col2})>=${sevenAgoTimestampS}) t1
         | group by t1.organization
       """.stripMargin
    spark.sql(sevenday_perOrg)
  }

  def getSmidSevendayAllOrg(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val sevenAgoTimestampE = d.getTime + Config.OneDay
    val sevenAgoTimestampS = sevenAgoTimestampE - Config.SevenDays
    spark.sql(
      s"""
         | select "all" as organization, smid, smid_os from reporting.smid where bigint(i_smid_last_active_time)>=${sevenAgoTimestampS}
      """.stripMargin).createOrReplaceTempView("smidsevenallorg")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_smid_cnt_per_org_7d from smidsevenallorg").createOrReplaceTempView("allsevsmid")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_android_cnt_per_org_7d from (select 'all' as organization, smid from smidsevenallorg where smid_os='android') t1 group by organization").createOrReplaceTempView("ads")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_ios_cnt_per_org_7d from (select 'all' as organization, smid from smidsevenallorg where smid_os='ios') t1 group by organization").createOrReplaceTempView("ioss")
    spark.sql(
      """
        | select t1.organization, t1.i_android_cnt_per_org_7d, t1.i_ios_cnt_per_org_7d,allsevsmid.i_smid_cnt_per_org_7d
        | from
        | (select ads.organization, ads.i_android_cnt_per_org_7d, ioss.i_ios_cnt_per_org_7d
        | from ads full join ioss on ads.organization=ioss.organization) t1
        | full join allsevsmid on allsevsmid.organization=t1.organization
      """.stripMargin)
  }

  def getThirtyDayAllOrg(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ThirtyAgoTimestampE = d.getTime + Config.OneDay
    val ThirtyAgoTimestampS = ThirtyAgoTimestampE - Config.ThirtyDays

    val thirtyay_perOrg =
      s"""
         | select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_30d
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where bigint(${col2})>=${ThirtyAgoTimestampS} ) t1
         | group by t1.organization
       """.stripMargin
    spark.sql(thirtyay_perOrg)
  }

  def getSmidThirtyAllOrg(spark: SparkSession, date: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ThirtyAgoTimestampE = d.getTime + Config.OneDay
    val ThirtyAgoTimestampS = ThirtyAgoTimestampE - Config.ThirtyDays
    spark.sql(
      s"""
         | select "all" as organization, smid, smid_os from reporting.smid where bigint(i_smid_last_active_time)>=${ThirtyAgoTimestampS}
      """.stripMargin).createOrReplaceTempView("smidthirtyallorg")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_smid_cnt_per_org_30d from smidthirtyallorg").createOrReplaceTempView("allthirtysmid")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_android_cnt_per_org_30d from (select 'all' as organization, smid from smidthirtyallorg where smid_os='android') t1 group by organization").createOrReplaceTempView("adt")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_ios_cnt_per_org_30d from (select 'all' as organization, smid from smidthirtyallorg where smid_os='ios') t1 group by organization").createOrReplaceTempView("iost")
    spark.sql(
      """
        | select t1.*,allthirtysmid.i_smid_cnt_per_org_30d
        | from
        | (select adt.organization, adt.i_android_cnt_per_org_30d, iost.i_ios_cnt_per_org_30d
        | from adt full join iost on adt.organization=iost.organization) t1
        | full join allthirtysmid on allthirtysmid.organization=t1.organization
      """.stripMargin)
  }

  def getTotalAllOrg(spark: SparkSession, date: String, tablename: String, col1: String): DataFrame = {
    val total_perOrg =
      s"""
         | select t1.organization, count(distinct(${col1})) as i_${col1}_cnt_per_org_total
         | from
         | (select "all" as organization, ${col1} from ${tablename}) t1
         | group by t1.organization
       """.stripMargin
    spark.sql(total_perOrg)
  }

  def getSmidTotalAllOrg(spark: SparkSession, date: String): DataFrame = {
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_smid_cnt_per_org_total from reporting.smid").createOrReplaceTempView("st")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_android_cnt_per_org_total from (select 'all' as organization, smid from reporting.smid where smid_os='android') group by organization").createOrReplaceTempView("adtt")
    spark.sql("select 'all' as organization, count(distinct(smid)) as i_ios_cnt_per_org_total from (select 'all' as organization, smid from reporting.smid where smid_os='ios') group by organization").createOrReplaceTempView("iostt")
    spark.sql(
      """
        | select t1.*,  st.i_smid_cnt_per_org_total
        | from
        | (select adtt.organization, adtt.i_android_cnt_per_org_total, iostt.i_ios_cnt_per_org_total
        | from adtt full join iostt on adtt.organization=iostt.organization) t1
        | full join st on st.organization=t1.organization
      """.stripMargin)
  }


  /**
    * black ID informations of each org
    * @param spark
    * @param date
    * @param tablename
    * @param col1
    * @param col2
    * @return
    */
  def getOneDayBlack(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val OneAgoTimestampS = d.getTime
    val OneAgoTimestampE = OneAgoTimestampS + Config.OneDay
    val oneday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_1d
         | from
         | (select organization,${col1}
         |  from ${tablename} where is_black=1 and bigint(${col2})>=${OneAgoTimestampS} ) t1
         |  group by t1.organization
        """.stripMargin
    spark.sql(oneday_perOrg)
  }

  def getSevenDaysBlack(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val sevenAgoTimestampE = d.getTime + Config.OneDay
    val sevenAgoTimestampS = sevenAgoTimestampE - Config.SevenDays
    val sevenday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_7d
         | from
         | (select organization,${col1}
         |  from ${tablename} where is_black=1 and bigint(${col2})>=${sevenAgoTimestampS} ) t1
         |  group by t1.organization
      """.stripMargin
    spark.sql(sevenday_perOrg)
  }

  def getThirtyDaysBlack(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val thirtyAgoTimestampE = d.getTime + Config.OneDay
    val thirtyAgoTimestampS = thirtyAgoTimestampE - Config.ThirtyDays
    val thirtyday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_30d
         | from
         | (select organization,${col1}
         |  from ${tablename} where is_black=1 and bigint(${col2})>=${thirtyAgoTimestampS} ) t1
         |  group by t1.organization
      """.stripMargin
    spark.sql(thirtyday_perOrg)
  }

  def getTotalBlack(spark: SparkSession, date: String, tablename: String,col1: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val total_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_total
         | from
         | (select organization,${col1}
         |  from ${tablename} where is_black=1) t1
         |  group by t1.organization
      """.stripMargin
    spark.sql(total_perOrg)
  }


  /**
    * the black ID informations of all orgs
    * @param spark
    * @param date
    * @param tablename
    * @param col1
    * @param col2
    * @return
    */
  def getOneDayBlackAllOrg(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val OneAgoTimestamp = d.getTime - Config.OneDay

    val oneday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_1d
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where is_black=1 and bigint(${col2})>=${OneAgoTimestamp}) t1
         | group by t1.organization
        """.stripMargin
    spark.sql(oneday_perOrg)
  }

  def getSevenDayBlackAllOrg(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val SevenAgoTimestamp = d.getTime - Config.SevenDays

    val sevenday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_7d
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where is_black=1 and bigint(${col2})>=${SevenAgoTimestamp}) t1
         | group by t1.organization
        """.stripMargin
    spark.sql(sevenday_perOrg)
  }

  def getThirtyDayBlackAllOrg(spark: SparkSession, date: String, tablename: String, col1: String, col2: String): DataFrame = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ThirtyAgoTimestamp = d.getTime - Config.ThirtyDays

    val thirtyday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_30d
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where is_black=1 and bigint(${col2})>=${ThirtyAgoTimestamp}) t1
         | group by t1.organization
        """.stripMargin
    spark.sql(thirtyday_perOrg)
  }

  def getTotalBlackAllOrg(spark: SparkSession, date: String, tablename: String, col1: String): DataFrame = {

    val totalday_perOrg =
      s"""
         | select t1.organization,count(distinct(${col1})) as i_black_${col1}_cnt_per_org_total
         | from
         | (select "all" as organization,${col1}
         | from ${tablename} where is_black=1 ) t1
         | group by t1.organization
        """.stripMargin
    spark.sql(totalday_perOrg)
  }
}
