package Spark2.Chapter_8Eight

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime

import scala.util.Random

object SparkSQLDataManually {

  val channelNames = Array("spark", "scala", "kafka", "Flink", "hadoop", "Storm", "Hive", "Impala", "HBase", "ML")
  val actionNames = Array("View", "Register")
  var riqiFormated = ""

  def main(args: Array[String]): Unit = {

//  生成日志数量与路径
    var numberItems: Int = 500
    var path = ""

    if (args.length > 0) {
      numberItems = args(0).toInt
      path = args(1)
    }
    println("USer log number is: " + numberItems)

//  日志生成时间
    riqiFormated = DateTime.now.minusDays(1).toString("yyyy-MM-dd")
//    userlogs(numberItems, path)
    var userLogBuffer = new StringBuffer(" ")

    //  生成网站日志函数
    def userlogs(numberItems: Long, path: String): Unit = {
      val random = new Random()

      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timestamp = df.format(new Date())
      val userID = random.nextInt((numberItems * 1000).toInt)
      val pageID = random.nextInt(numberItems.toInt)
      val channel = channelNames(random.nextInt(10))
      val action = actionNames(random.nextInt(2))

      userLogBuffer.append(timestamp).append("\t")
        .append(userID).append("\t")
        .append(pageID).append("\t")
        .append(channel).append("\t")
        .append(action).append("\n")
    }

    val printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path + "userLog.log")))

    try {
      printWriter.write(userLogBuffer.toString())
    } catch {
      case e: Exception => println(e.toString)
    } finally {
      printWriter.close()
    }

  }

}