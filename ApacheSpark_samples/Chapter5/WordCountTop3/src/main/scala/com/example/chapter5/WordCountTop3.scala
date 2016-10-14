package com.example.chapter5

import org.apache.spark.{SparkConf, SparkContext}

object WordCountTop3 {

  def main(args: Array[String]) {
    require(args.length >= 1,
      "ドライバプログラムの引数に単語をカウントする" +
      "ファイルへのパスを指定してください")

    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      // 単語ごとに(単語, 出現回数)のタプルを作る
      val filePath = args(0)
      val wordAndCountRDD = sc.textFile(filePath)
                              .flatMap(_.split("[ ,.]"))
                              .filter(_.matches("""\p{Alnum}+"""))
                              .map((_, 1))
                              .reduceByKey(_ + _)
      
      // 出現回数が多い3つの単語を見つける
      val top3Words = wordAndCountRDD.map {
        case (word, count) => (count, word)
      }.sortByKey(false).map {
        case (count, word) => (word, count)
      }.take(3)

      // 出現回数が多い単語トップ3を標準出力にプリントする
      top3Words.foreach(println)
    } finally {
      sc.stop()
    }
  }
}
    
