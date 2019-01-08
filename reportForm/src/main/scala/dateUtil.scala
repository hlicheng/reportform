import java.text.SimpleDateFormat

object dateUtil {

  var now: String = ""
  var onedayago: String = ""


  def getDate(date: String, daysnum: Int): String = {
    val pattern = "yyyyMMdd"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ondago = d.getTime - Config.OneDay * daysnum
    new SimpleDateFormat(pattern).format(ondago)
  }

}