import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Row
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder

case class Weather( date: String,
                    day_of_week: String,
                    avg_temp: Double,
                    max_temp: Double,
                    min_temp: Double,
                    rainfall: Double,
                    daylight_hours: Double,
                    max_depth_snowfall: Double,
                    total_snowfall: Double,
                    solar_radiation: Double,
                    mean_wind_speed: Double,
                    max_wind_speed: Double,
                    max_instantaneous_wind_speed: Double,
                    avg_humidity: Double,
                    avg_cloud_cover: Double)

case class Sales(date: String, sales: Double)

case class Predict(describe: String,
                   avg_temp: Double,
                   rainfall: Double,
                   weekend: Double,
                   total_snowfall: Double )

object MLPipelineLRExample {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("MLPipelineLRExample")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // 気候データの読み込みとDataFrame変換
    val weatherCSVRDD = sc.textFile("data/weather.csv")
    val headerOfWeatherCSVRDD = sc.parallelize(Array(weatherCSVRDD.first))
    val weatherCSVwithoutHeaderRDD = weatherCSVRDD.subtract(headerOfWeatherCSVRDD)
    val weatherDF = weatherCSVwithoutHeaderRDD.map(_.split(",")).
      map(p => Weather(p(0),
      p(1),
      p(2).trim.toDouble,
      p(3).trim.toDouble,
      p(4).trim.toDouble,
      p(5).trim.toDouble,
      p(6).trim.toDouble,
      p(7).trim.toDouble,
      p(8).trim.toDouble,
      p(9).trim.toDouble,
      p(10).trim.toDouble,
      p(11).trim.toDouble,
      p(12).trim.toDouble,
      p(13).trim.toDouble,
      p(14).trim.toDouble
    )).toDF()

    // 売上データの読み込みとDataFrame変換
    val salesCSVRDD = sc.textFile("data/sales.csv")
    val headerOfSalesCSVRDD = sc.parallelize(Array(salesCSVRDD.first))
    val salesCSVwithoutHeaderRDD = salesCSVRDD.subtract(headerOfSalesCSVRDD)

    val salesDF = salesCSVwithoutHeaderRDD.map(_.split(",")).map(p => Sales(p(0), p(1).trim.toDouble)).toDF()

    // データの前処理
    val salesAndWeatherDF = salesDF.join(weatherDF, "date")

    val isWeekend = udf((t: String) => if(t.contains("日") || t.contains("土")) 1d else 0d)

    // ここまでは8.5節と同じ
    val replacedSalesAndWeatherDF = salesAndWeatherDF.withColumn("weekend", isWeekend(salesAndWeatherDF("day_of_week"))).drop("day_of_week")

    // パイプラインの作成
    val va = new VectorAssembler()
      .setInputCols{
      Array("avg_temp", "weekend", "rainfall")
    }.setOutputCol("input_vec")

    val scaler = new StandardScaler()
      .setInputCol(va.getOutputCol)
      .setOutputCol("scaled_vec")

    //Transformerに指定可能なパラメータの確認
    //va.explainParams
    //scaler.explainParams

    val lr = new LinearRegression()
      .setMaxIter(3)
      .setFeaturesCol(scaler.getOutputCol)
      .setLabelCol("sales")

    val pipeline = new Pipeline().setStages(Array(va, scaler, lr))
    val pipelineModel = pipeline.fit(replacedSalesAndWeatherDF)

    val test = sc.parallelize(Seq(
      Predict("Usual Day",20.0,20,0,0),
      Predict("Weekend",20.0,20,1,0),
      Predict("Cold day",3.0,20,0,20)
    )).toDF

    val predictedDataDF = pipelineModel.transform(test)

    val descAndPred = predictedDataDF.select("describe", "prediction").collect()

    println("####### パラメータチューニング適用前 #######")

    descAndPred.foreach {
      case Row(describe: String, prediction: Double) =>
        println(s"($describe) -> prediction=$prediction")
    }

    // 精度のチューニング
    val crossval = new CrossValidator().setEstimator(pipeline)
    val re = new RegressionEvaluator().setLabelCol("sales")
    crossval.setEvaluator(re)

    val paramGrid = new ParamGridBuilder()

    paramGrid.addGrid(va.inputCols, Array(
      Array("avg_temp","weekend","rainfall"),
      Array("avg_temp","weekend","rainfall","total_snowfall")
    ))
    paramGrid.addGrid(lr.maxIter, Array(5,10))
    paramGrid.addGrid(scaler.withMean, Array(false))

    crossval.setEstimatorParamMaps(paramGrid.build())
    val cvModel = crossval.fit(replacedSalesAndWeatherDF)

    println("####### 交差検定＋グリッドサーチ適用後 #######")
    cvModel.transform(test).select("describe", "prediction")
      .collect()
      .foreach {
      case Row(describe: String, prediction: Double) =>
        println(s"($describe) -> prediction=$prediction")
    }
  }
}