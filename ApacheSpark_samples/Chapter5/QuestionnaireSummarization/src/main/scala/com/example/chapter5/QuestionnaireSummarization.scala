package com.example.chapter5

import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object QuestionnaireSummarization {

  /**
   * アンケート全ての評価の平均値を求めるメソッド
   */ 
  private def computeAllAvg(rdd: RDD[(Int, String, Int)]) = {
    val (totalPoint, count) =
      rdd.map(record => (record._3, 1)).reduce {
        case ((intermedPoint, intermedCount), (point, one)) =>
          (intermedPoint + point, intermedCount + one)
      }
    totalPoint / count.toDouble
  }

  /**
   * 世代別の評価の平均値を求めるメソッド
   */
  private def computeAgeRangeAvg(rdd: RDD[(Int, String, Int)]) = {
    rdd.map(record => (record._1, (record._3, 1))).reduceByKey {
      case ((intermedPoint, intermedCount), (point, one)) =>
        (intermedPoint + point, intermedCount + one)
    }.map {
      case (ageRange, (totalPoint, count)) =>
        (ageRange, totalPoint / count.toDouble)
    }.collect
  }

  /**
   * 男女別の評価の平均値を求めるメソッド
   */
  private def computeMorFAvg(
      rdd: RDD[(Int, String, Int)],
      numMAcc: Accumulator[Int],
      totalPointMAcc: Accumulator[Int],
      numFAcc: Accumulator[Int],
      totalPointFAcc: Accumulator[Int]) = {
    rdd.foreach {
      case (_, maleOrFemale, point) =>
        maleOrFemale match {
          case "M" =>
            numMAcc += 1
            totalPointMAcc += point
          case "F" =>
            numFAcc += 1
            totalPointFAcc += point
        }
    }
    Seq(("Male", totalPointMAcc.value / numMAcc.value.toDouble),
      ("Female", totalPointFAcc.value / numFAcc.value.toDouble))
  }

  def main(args: Array[String]) {
    require(
      args.length >= 2,
      """
        |アプリケーションの引数に
        |<アンケートのCSVファイルのパス>
        |<結果の出力先のパス>を指定してください。""".stripMargin)

    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      val filePath = args(0)

      // アンケートをロードし、(年代, 性別, 評価)の
      // タプルを要素とするRDDを生成する。
      val questionnaireRDD = sc.textFile(filePath).map { record =>
        val splitRecord = record.split(",")
        val ageRange = splitRecord(0).toInt / 10 * 10
        val maleOrFemale = splitRecord(1)
        val point = splitRecord(2).toInt
        (ageRange, maleOrFemale, point)
      }

      // questionnaireRDDは各集計処理で利用されるためキャッシュする
      questionnaireRDD.cache

      // 評価の全平均値を計算する
      val avgAll = computeAllAvg(questionnaireRDD)
      // 年代別の平均値を計算する
      val avgAgeRange = computeAgeRangeAvg(questionnaireRDD)

      // 性別がMのアンケートの数をカウントするアキュムレータ
      val numMAcc = sc.accumulator(0, "Number of M")
      // 性別がMのアンケートの評価の合計をカウントするアキュムレータ
      val totalPointMAcc = sc.accumulator(0, "Total Point of M")
      // 性別がFのアンケートの数をカウントするアキュムレータ
      val numFAcc = sc.accumulator(0, "Number of F")
      // 性別がFのアンケートの評価の合計をカウントするアキュムレータ
      val totalPointFAcc = sc.accumulator(0, "TotalPoint of F")

      // 性別ごとの平均値を計算する
      val avgMorF = computeMorFAvg(
        questionnaireRDD,
        numMAcc,
        totalPointMAcc,
        numFAcc,
        totalPointFAcc)

      println(s"AVG ALL: $avgAll")
      avgAgeRange.foreach {
        case (ageRange, avg) =>
          println(s"AVG Age Range($ageRange): $avg")
      }

      avgMorF.foreach {
        case (mOrF, avg) =>
          println(s"AVG $mOrF: $avg")
      }
    } finally {
      sc.stop()
    }
  }
}




