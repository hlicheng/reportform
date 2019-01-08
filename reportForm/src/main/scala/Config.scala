object Config {

  final val TokenIdisBlcak: Long = 700.toLong
  final val IpisBlack: Long = (5184*1000000).toLong

  val aePath = "/user/data/event/detail_ae/dt=%s/*"
  val captchaPath = "/user/data/event/detail_captcha/dt=%s/*"
  val fpPath = "/user/data/event/detail_fp/dt=%s/*"



  val filterKey = List[Any](None,"None",null,""," ","Null","null","#")
  val filterOs = List[String]("android","ios")
  val shumeiOrgs = Set(
    "RlokQwRlVjUrTUlkIqOg",
    "fe1iHzsMKi3vHLmqtHyl",
    "pLwbu943053KXGa9HdkD",
    "9h4YLrU1SDTN7c2srruX",
    "d6tpAY1oV0Kv5jRSgxQr",
    "xA9oIZc7py5sR51iWO5C"
  )

  val OneDay: Long = 86400000
  val SevenDays: Long = 604800000
  val ThirtyDays: Long = "2592000000".toLong

  val backupNum: Int = 14



  val tokenIdSql1 =s"""
                    |create table if not exists reporting.temptokenid
                    |(organization STRING,
                    |tokenid STRING,
                    |i_tokenid_last_active_time STRING,
                    |is_black INT)
                    | stored as parquet
                  """.stripMargin

  val tokenIdSql2 =s"""
                     | INSERT OVERWRITE table reporting.temptokenid
                     | select t4.organization, t4.tokenid, t4.i_tokenid_last_active_time, t4.is_black
                     | from
                     | (
                     | select t3.*, row_number() over (partition by t3.organization, t3.tokenid order by t3.i_tokenid_last_active_time desc) as rank
                     | from
                     | (
                     | select if(t1.organization is null,t2.organization,t1.organization) as organization,
                     |        if(t1.tokenid is null, t2.tokenid, t1.tokenid) as tokenid,
                     |        if((t1.i_tokenid_last_active_time is not null and t1.i_tokenid_last_active_time<t2.i_tokenid_last_active_time) or t1.i_tokenid_last_active_time is null, t2.i_tokenid_last_active_time, t1.i_tokenid_last_active_time) as i_tokenid_last_active_time,
                     |        if((t1.is_black is not null and t1.is_black<t2.is_black) or t1.is_black is null,t2.is_black,t1.is_black) as is_black
                     |     from (select * from tokenid_now) t1 full join (select organization,tokenid,i_tokenid_last_active_time,is_black from reporting.tokenid where dt=%s) t2
                     | on t1.organization=t2.organization and t1.tokenid=t2.tokenid
                     | ) t3
                     | ) t4
                     | where t4.rank=1 and t4.organization is not null
                   """.stripMargin

  val tokenIdSql3 =
    s"""
      |insert into table reporting.tokenid partition (dt=%s) (select * from reporting.temptokenid)
    """.stripMargin

//  val tokenIdSql5 =
//    """
//      |alter table reporting.temptokenid rename to reporting.tokenid
//    """.stripMargin

  val smidSql1 ="""
                | create table if not exists reporting.tempsmid
                | (organization STRING,
                | smid STRING,
                | smid_os STRING,
                | i_smid_last_active_time STRING)
                | stored as parquet
              """.stripMargin

  val smidSql2 =s"""
                    | INSERT OVERWRITE TABLE reporting.tempsmid
                    | SELECT t4.organization, t4.smid, t4.smid_os, t4.i_smid_last_active_time
                    | FROM
                    | (
                    | SELECT t3.*,row_number() over (partition BY t3.organization, t3.smid ORDER BY t3.i_smid_last_active_time DESC ) as rank
                    | FROM
                    | (
                    | select if(t1.organization is null,t2.organization,t1.organization) as organization,
                    |        if(t1.smid is null, t2.smid, t1.smid) as smid,
                    |        if(t1.smid_os is null, t2.smid_os, t1.smid_os) as smid_os,
                    |        if((t1.i_smid_last_active_time is not null and t1.i_smid_last_active_time < t2.i_smid_last_active_time) or t1.i_smid_last_active_time is null, t2.i_smid_last_active_time, t1.i_smid_last_active_time) as i_smid_last_active_time
                    | from (SELECT organization,smid,smid_os,i_smid_last_active_time FROM reporting.Smid where dt=%s) t1 FULL JOIN (SELECT * FROM smid_now) t2
                    | ON t1.organization=t2.organization AND t1.smid=t2.smid
                    | ) t3
                    | ) t4
                    | WHERE t4.rank=1 and t4.organization is not null
                  """.stripMargin

  val smidSql3 =
    s"""
      |insert into table reporting.smid partition (dt=%s) select * from reporting.tempsmid
    """.stripMargin

//  val smidSql4 ="""
//                  |drop table reporting.smid
//                """.stripMargin
//  val smidSql5 =
//    """
//      |alter table reporting.tempsmid rename to reporting.smid
//    """.stripMargin

  val ipSql1 ="""
                |create table if not exists reporting.tempip
                |(organization STRING,
                |ip STRING,
                |i_ip_last_active_time STRING,
                |is_black INT)
                | stored as parquet
              """.stripMargin

  val ipSql2 =s"""
                | INSERT overwrite table reporting.tempip
                | SELECT t4.organization,t4.ip,t4.i_ip_last_active_time,t4.is_black
                | FROM
                | (SELECT t3.*,row_number() over (partition BY t3.organization, t3.ip ORDER BY t3.i_ip_last_active_time desc) as rank
                | FROM
                | (
                | select if(t1.organization is null,t2.organization,t1.organization) as organization,
                |        if(t1.ip is null, t2.ip, t1.ip) as ip,
                |        if((t1.i_ip_last_active_time is not null and t1.i_ip_last_active_time<t2.i_ip_last_active_time) or t1.i_ip_last_active_time is null, t2.i_ip_last_active_time, t1.i_ip_last_active_time) as i_ip_last_active_time,
                |        if((t1.is_black is not null and t1.is_black<t2.is_black) or t1.is_black is null,t2.is_black,t1.is_black) as is_black
                |  from (SELECT organization,ip,i_ip_last_active_time,is_black FROM reporting.Ip where dt=%s) t1
                | FULL JOIN
                | (SELECT * FROM ip_now) t2
                | ON t1.organization=t2.organization AND t1.ip=t2.ip) t3
                | ) t4
                | WHERE t4.rank=1 and t4.organization is not null
                |
              """.stripMargin

  val ipSql3 =
    s"""
      |insert into table reporting.ip partition (dt=%s) select * from reporting.tempip
    """.stripMargin

//  val ipSql4 ="""
//                | drop table reporting.Ip
//              """.stripMargin
//  val ipSql5 =
//    """
//      |alter table reporting.tempip rename to reporting.Ip
//    """.stripMargin

  val phoneSql1 =
    """
      | create table if not exists reporting.tempphone
      | (organization STRING,
      | phone STRING,
      | i_phone_last_active_time STRING,
      | is_black INT)
      | stored as parquet
    """.stripMargin

  val phoneSql2 =
    s"""
      | INSERT overwrite table reporting.tempphone
      | SELECT t4.organization, t4.phone, t4.i_phone_last_active_time, t4.is_black
      | FROM
      | (
      | SELECT t3.*,row_number() over (partition BY t3.organization,t3.phone ORDER BY t3.i_phone_last_active_time DESC ) as rank
      | FROM
      | (
      | select if(t1.organization is null,t2.organization,t1.organization) as organization,
      |        if(t1.phone is null, t2.phone, t1.phone) as phone,
      |        if((t1.i_phone_last_active_time is not null and t1.i_phone_last_active_time<t2.i_phone_last_active_time) or t1.i_phone_last_active_time is null, t2.i_phone_last_active_time, t1.i_phone_last_active_time) as i_phone_last_active_time,
      |        if((t1.is_black is not null and t1.is_black<t2.is_black) or t1.is_black is null,t2.is_black,t1.is_black) as is_black
      | from (SELECT organization,phone,i_phone_last_active_time,is_black FROM reporting.phone where dt=%s) t1 FULL JOIN (SELECT * FROM phone_now) t2
      | ON t1.organization=t2.organization AND t1.phone=t2.phone
      | ) t3
      | ) t4
      | where t4.rank=1 and t4.organization is not null
    """.stripMargin

  val phoneSql3 =
    s"""
      |insert into table reporting.phone partition (dt=%s) select * from reporting.tempphone
    """.stripMargin

//  val phoneSql4 =
//    """
//      |drop table reporting.phone
//    """.stripMargin
//  val phoneSql5 =
//    """
//      |alter table reporting.tempphone rename to reporting.phone
//    """.stripMargin


}
