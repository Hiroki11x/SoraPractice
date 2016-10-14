package com.example.chapter4
import org.joda.time.{DateTime, DateTimeConstants} 
import org.joda.time.format.DateTimeFormat
import org.apache.spark.{SparkConf, SparkContext}

object SundayCount {
  def main(args: Array[String]) { 
    if (args.length < 1) {
      throw new IllegalArgumentException( "コマンドの引数に日付が記録されたファイルへのパスを入力してください")
    }
    val filePath = args(0)
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    try {
      val textRDD = sc.textFile(filePath)
      val dateTimeRDD = textRDD.map { dateStr =>
        val pattern =
          DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }
      val sundayRDD = dateTimeRDD.filter { dateTime => 
        dateTime.getDayOfWeek == DateTimeConstants.SUNDAY
      }
      val numOfSunday = sundayRDD.count 
      println(s"与えられたデータの中に日曜日は${numOfSunday}個含まれていました")
    } finally { 
      sc.stop()
    } 
  }
}
