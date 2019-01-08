import org.apache.spark.sql.{DataFrame, SparkSession}

object getblackIndex {

  def getOneDayPerOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getOneDayBlack(spark, date, "reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ipblack")
    Calculate.getOneDayBlack(spark, date, "reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phoneblack")
    Calculate.getOneDayBlack(spark, date, "reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("tokenblack")

    val onedayblack =
      """
        | select if(t1.organization is null,tokenblack.organization,t1.organization) as organization,t1.i_black_ip_cnt_per_org_1d,
        |   t1.i_black_phone_cnt_per_org_1d, tokenblack.i_black_tokenid_cnt_per_org_1d
        | from
        | (select if(ipblack.organization is null,phoneblack.organization,ipblack.organization) as organization,ipblack.i_black_ip_cnt_per_org_1d,
        |   phoneblack.i_black_phone_cnt_per_org_1d from ipblack full join phoneblack on ipblack.organization=phoneblack.organization) t1
        | full join tokenblack on t1.organization=tokenblack.organization
      """.stripMargin
    spark.sql(onedayblack)
  }

  def getSevenDayPerOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSevenDaysBlack(spark, date, "reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ipblack2")
    Calculate.getSevenDaysBlack(spark, date, "reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phoneblack2")
    Calculate.getSevenDaysBlack(spark, date, "reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("tokenblack2")

    val onedayblack =
      """
        | select if(t1.organization is null,tokenblack2.organization,t1.organization) as organization,t1.i_black_ip_cnt_per_org_7d,
        |   t1.i_black_phone_cnt_per_org_7d, tokenblack2.i_black_tokenid_cnt_per_org_7d
        | from
        | (select if(ipblack2.organization is null,phoneblack2.organization,ipblack2.organization) as organization,ipblack2.i_black_ip_cnt_per_org_7d,
        |   phoneblack2.i_black_phone_cnt_per_org_7d from ipblack2 full join phoneblack2 on ipblack2.organization=phoneblack2.organization) t1
        | full join tokenblack2 on t1.organization=tokenblack2.organization
      """.stripMargin
    spark.sql(onedayblack)
  }

  def getThirtyDayPerOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getThirtyDaysBlack(spark, date, "reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ipblack3")
    Calculate.getThirtyDaysBlack(spark, date, "reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phoneblack3")
    Calculate.getThirtyDaysBlack(spark, date, "reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("tokenblack3")

    val onedayblack =
      """
        | select if(t1.organization is null,tokenblack3.organization,t1.organization) as organization,t1.i_black_ip_cnt_per_org_30d,
        |   t1.i_black_phone_cnt_per_org_30d, tokenblack3.i_black_tokenid_cnt_per_org_30d
        | from
        | (select if(ipblack3.organization is null,phoneblack3.organization,ipblack3.organization) as organization,ipblack3.i_black_ip_cnt_per_org_30d,
        |   phoneblack3.i_black_phone_cnt_per_org_30d from ipblack3 full join phoneblack3 on ipblack3.organization=phoneblack3.organization) t1
        | full join tokenblack3 on t1.organization=tokenblack3.organization
      """.stripMargin
    spark.sql(onedayblack)
  }

  def getTotalPerOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getTotalBlack(spark, date, "reporting.ip","ip").createOrReplaceTempView("ipblack4")
    Calculate.getTotalBlack(spark, date, "reporting.phone","phone").createOrReplaceTempView("phoneblack4")
    Calculate.getTotalBlack(spark, date, "reporting.tokenid","tokenid").createOrReplaceTempView("tokenblack4")

    val onedayblack =
      """
        | select if(t1.organization is null,tokenblack4.organization,t1.organization) as organization,t1.i_black_ip_cnt_per_org_total,
        |   t1.i_black_phone_cnt_per_org_total, tokenblack4.i_black_tokenid_cnt_per_org_total
        | from
        | (select if(ipblack4.organization is null,phoneblack4.organization,ipblack4.organization) as organization,ipblack4.i_black_ip_cnt_per_org_total,
        |   phoneblack4.i_black_phone_cnt_per_org_total from ipblack4 full join phoneblack4 on ipblack4.organization=phoneblack4.organization) t1
        | full join tokenblack4 on t1.organization=tokenblack4.organization
      """.stripMargin
    spark.sql(onedayblack)
  }

  def getIndexPerOrgBlack(spark: SparkSession, date: String): DataFrame = {
    getOneDayPerOrgBlack(spark, date).createOrReplaceTempView("oneday")
    getSevenDayPerOrgBlack(spark, date).createOrReplaceTempView("sevendays")
    getThirtyDayPerOrgBlack(spark, date).createOrReplaceTempView("thirtydays")
    getTotalPerOrgBlack(spark, date).createOrReplaceTempView("total")

    val indexOrgBlack =
      s"""
        | select if(t2.organization is null,total.organization,t2.organization) as organization, t2.i_black_ip_cnt_per_org_1d, t2.i_black_phone_cnt_per_org_1d,t2.i_black_tokenid_cnt_per_org_1d,
        |               t2.i_black_ip_cnt_per_org_7d, t2.i_black_phone_cnt_per_org_7d,t2.i_black_tokenid_cnt_per_org_7d,
        |      t2.i_black_ip_cnt_per_org_30d, t2.i_black_phone_cnt_per_org_30d,t2.i_black_tokenid_cnt_per_org_30d,
        |       total.i_black_ip_cnt_per_org_total, total.i_black_phone_cnt_per_org_total,total.i_black_tokenid_cnt_per_org_total,${date} as date
        | from
        | (select if(t1.organization is null,thirtydays.organization,t1.organization) as organization, t1.i_black_ip_cnt_per_org_1d, t1.i_black_phone_cnt_per_org_1d,t1.i_black_tokenid_cnt_per_org_1d,
        |               t1.i_black_ip_cnt_per_org_7d, t1.i_black_phone_cnt_per_org_7d,t1.i_black_tokenid_cnt_per_org_7d,
        |      thirtydays.i_black_ip_cnt_per_org_30d, thirtydays.i_black_phone_cnt_per_org_30d,thirtydays.i_black_tokenid_cnt_per_org_30d
        | from
        | (select if(oneday.organization is null,sevendays.organization,oneday.organization) as organization,oneday.i_black_ip_cnt_per_org_1d, oneday.i_black_phone_cnt_per_org_1d,oneday.i_black_tokenid_cnt_per_org_1d,
        |               sevendays.i_black_ip_cnt_per_org_7d, sevendays.i_black_phone_cnt_per_org_7d,sevendays.i_black_tokenid_cnt_per_org_7d
        | from oneday full join sevendays on oneday.organization=sevendays.organization) t1
        | full join thirtydays on t1.organization=thirtydays.organization) t2
        | full join total on t2.organization=total.organization
      """.stripMargin

    spark.sql(indexOrgBlack)

  }




  def getOneDayAllOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getOneDayBlackAllOrg(spark, date,"reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ipall")
    Calculate.getOneDayBlackAllOrg(spark, date,"reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phoneall")
    Calculate.getOneDayBlackAllOrg(spark, date,"reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("tokenall")
    val oneblackall =
      """
        | select t1.*, tokenall.i_black_tokenid_cnt_per_org_1d
        | from
        | (select ipall.*, phoneall.i_black_phone_cnt_per_org_1d from ipall full join phoneall on ipall.organization=phoneall.organization ) t1
        | full join tokenall on  tokenall.organization=t1.organization
      """.stripMargin
    spark.sql(oneblackall)
  }

  def getSevenDayAllOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getSevenDayBlackAllOrg(spark, date,"reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ipall2")
    Calculate.getSevenDayBlackAllOrg(spark, date,"reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phoneall2")
    Calculate.getSevenDayBlackAllOrg(spark, date,"reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("tokenall2")
    val sevenblackall =
      """
        | select t1.*, tokenall2.i_black_tokenid_cnt_per_org_7d
        | from
        | (select ipall2.*, phoneall2.i_black_phone_cnt_per_org_7d from ipall2 full join phoneall2 on ipall2.organization=phoneall2.organization ) t1
        | full join tokenall2 on tokenall2.organization=t1.organization
      """.stripMargin
    spark.sql(sevenblackall)
  }

  def getThirtyDayAllOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getThirtyDayBlackAllOrg(spark, date,"reporting.ip","ip", "i_ip_last_active_time").createOrReplaceTempView("ipall3")
    Calculate.getThirtyDayBlackAllOrg(spark, date,"reporting.phone","phone", "i_phone_last_active_time").createOrReplaceTempView("phoneall3")
    Calculate.getThirtyDayBlackAllOrg(spark, date,"reporting.tokenid","tokenid", "i_tokenid_last_active_time").createOrReplaceTempView("tokenall3")
    val thirtyblackall =
      """
        | select t1.*, tokenall3.i_black_tokenid_cnt_per_org_30d
        | from
        | (select ipall3.*, phoneall3.i_black_phone_cnt_per_org_30d from ipall3 full join phoneall3 on ipall3.organization=phoneall3.organization ) t1
        | full join tokenall3 on tokenall3.organization=t1.organization
      """.stripMargin
    spark.sql(thirtyblackall)
  }

  def getTotalAllOrgBlack(spark: SparkSession, date: String): DataFrame = {
    Calculate.getTotalBlackAllOrg(spark, date,"reporting.ip","ip").createOrReplaceTempView("ipall4")
    Calculate.getTotalBlackAllOrg(spark, date,"reporting.phone","phone").createOrReplaceTempView("phoneall4")
    Calculate.getTotalBlackAllOrg(spark, date,"reporting.tokenid","tokenid").createOrReplaceTempView("tokenall4")
    val totalblackall =
      """
        | select t1.*, tokenall4.i_black_tokenid_cnt_per_org_total
        | from
        | (select ipall4.*, phoneall4.i_black_phone_cnt_per_org_total from ipall4 full join phoneall4 on ipall4.organization=phoneall4.organization ) t1
        | full join tokenall4 on tokenall4.organization=t1.organization
      """.stripMargin
    spark.sql(totalblackall)
  }

  def getIndexAllOrgBlack(spark: SparkSession, date: String): DataFrame = {
    getOneDayAllOrgBlack(spark, date).createOrReplaceTempView("oneall")
    getSevenDayAllOrgBlack(spark, date).createOrReplaceTempView("sevenall")
    getThirtyDayAllOrgBlack(spark, date).createOrReplaceTempView("thirtyall")
    getTotalAllOrgBlack(spark, date).createOrReplaceTempView("totalall")
    val allorg =
      s"""
        | select t2.*, totalall.i_black_ip_cnt_per_org_total,totalall.i_black_phone_cnt_per_org_total,totalall.i_black_tokenid_cnt_per_org_total, ${date} as date
        | from
        | (select t1.*, thirtyall.i_black_ip_cnt_per_org_30d,thirtyall.i_black_phone_cnt_per_org_30d,thirtyall.i_black_tokenid_cnt_per_org_30d
        | from
        | (select oneall.*, sevenall.i_black_ip_cnt_per_org_7d,sevenall.i_black_phone_cnt_per_org_7d,sevenall.i_black_tokenid_cnt_per_org_7d
        | from oneall full join sevenall on oneall.organization=sevenall.organization) t1
        | full join thirtyall on t1.organization=thirtyall.organization ) t2
        | full join totalall on t2.organization=totalall.organization
      """.stripMargin
    spark.sql(allorg)
  }


  def getSumOrgBlack(spark: SparkSession, date: String): DataFrame = {
    val perOrgBlack = getIndexPerOrgBlack(spark,date)
    val allOrgBlack = getIndexAllOrgBlack(spark, date)
    perOrgBlack.union(allOrgBlack)
  }


}
