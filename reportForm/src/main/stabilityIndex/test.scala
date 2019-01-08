import java.text.SimpleDateFormat
import java.util.Calendar

object test {
  def main(args: Array[String]): Unit = {
    val pattern = "yyyyMMdd"
    val date = "20181225"
    val d = new SimpleDateFormat(pattern).parse(date)
    val ondago = d.getTime - Config.OneDay
    val res = new SimpleDateFormat(pattern).format(ondago)

  }
}
