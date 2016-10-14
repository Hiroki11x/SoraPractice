package com.example.chapter5

import scala.collection.mutable.{HashMap, Map}
import java.io.{BufferedReader, InputStreamReader, Reader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object BestSellerFinder {
  /**
   * 売り上げデータが記録されたファイルから、
   * (商品ID, 販売個数)を要素に持つRDDを生成するメソッド
   */
  private def createSalesRDD(csvFile: String, sc: SparkContext) = {
    val logRDD = sc.textFile(csvFile)
    logRDD.map { record =>
      val splitRecord = record.split(",")
      val productId = splitRecord(2)
      val numOfSold = splitRecord(3).toInt
      (productId, numOfSold)
    }
  }

  /**
   * ある月の売り上げデータを格納するRDDから
   * 50個以上売れた商品の情報を残したRDDを生成するメソッド  
   */
  private def createOver50SoldRDD(rdd: RDD[(String, Int)]) = {
    rdd.reduceByKey(_ + _).filter(_._2 >= 50)
  }

  /**
   * 商品のマスタデータをロードしたHashMapを生成するメソッド
   */
  private def loadCSVIntoMap(productsCSVFile: String) = {
    var productsCSVReader: Reader = null

    try {
      val productsMap = new HashMap[String, (String, Int)]
      val hadoopConf = new Configuration
      val fileSystem = FileSystem.get(hadoopConf)
      val inputStream = fileSystem.open(new Path(productsCSVFile))
      val productsCSVReader = new BufferedReader(new InputStreamReader(inputStream))
      var line = productsCSVReader.readLine

      while (line != null) {
        val splitLine = line.split(",")
        val productId = splitLine(0)
        val productName = splitLine(1)
        val unitPrice = splitLine(2).toInt
        productsMap(productId) = (productName, unitPrice)
        line = productsCSVReader.readLine
      }
      productsMap
    } finally {
      if (productsCSVReader != null) {
        productsCSVReader.close()
      }
    }
  }

  /**
   * 商品のマスタデータと結合して、
   * ふたつの月で50個以上売れた商品の情報を格納するRDDを生成するメソッド
   */
  private def createResultRDD(
      broadcastedMap: Broadcast[_ <: Map[String, (String, Int)]],
      rdd: RDD[(String, Int)]) = {
    rdd.map {
      case (productId, amount) =>
        val productsMap = broadcastedMap.value
        val (productName, unitPrice) = productsMap(productId)
        (productName, amount, amount * unitPrice)
    }
  }

  def main(args: Array[String]) {
    require(
      args.length >= 4,
      """
        |アプリケーションの引数に
        |<ひとつめの月の売り上げデータファイルのパス>
        |<ふたつめの月の売り上げデータファイルのパス>
        |<商品のマスタデータファイルのパス>
        |<結果の出力先のパス>を指定してください。""".stripMargin)

    val conf = new SparkConf
    val sc = new SparkContext(conf)

    try {
      // ドライバプログラムの引数に渡された値を
      // 一括して取り出す。
      val Array(
        salesCSVFile1,
        salesCSVFile2,
        productsCSVFile,
        outputPath) = args.take(4)

      // それぞれの月の売り上げが記録されたCSVファイルから
      // (商品ID, 販売個数)のタプルを要素とするRDDを生成する。
      val salesRDD1 = createSalesRDD(salesCSVFile1, sc)
      val salesRDD2 = createSalesRDD(salesCSVFile2, sc)

      // (商品ID, 販売個数)のタプルを要素とするRDDから、
      // (商品ID, 合計販売個数)のタプルを要素とするRDDを生成する。
      val over50SoldRDD1 = createOver50SoldRDD(salesRDD1)
      val over50SoldRDD2 = createOver50SoldRDD(salesRDD2)

      // 両方の月で50個上売れた商品について、
      // (商品ID, 両方の月の合計販売個数)のタプルを要素とするRDDを生成する。

      val bothOver50SoldRDD = over50SoldRDD1.join(over50SoldRDD2)
      val over50SoldAndAmountRDD = bothOver50SoldRDD.map {
        case (productId, (amount1, amount2)) =>
          (productId, amount1 + amount2)
      }

      // 商品のマスタデータの内容をHashMapにロードし、
      // ブロードキャスト変数としてエグゼキュータに配布する。
      val productsMap = loadCSVIntoMap(productsCSVFile)
      val broadcastedMap = sc.broadcast(productsMap)

      // 結果を求め、ファイルシステム上に出力する
      val resultRDD = createResultRDD(broadcastedMap, over50SoldAndAmountRDD)
      resultRDD.saveAsTextFile(outputPath)
    } finally {
      sc.stop()
    }
  }
}
