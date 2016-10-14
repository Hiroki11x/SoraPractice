import akka.actor.Props
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Milliseconds}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.sql.hive.HiveContext


object SparkBookStreamingAdvancedWithJDBCServer {
  def main(args: Array[String]) {

    if(args.length != 2) {
      System.err.println(
        "Usage: SparkBookStreamingAdvancedWithJDBCServer <host> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkBookStreamingAdvancedWithJDBCServer")
    val sc = new SparkContext(sparkConf)
    val batchDuration = Milliseconds(5000)
    val rememberDuration = Milliseconds(10000)
    val ssc = new StreamingContext(sc, batchDuration)
    ssc.remember(rememberDuration)

    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    ssc.checkpoint("/tmp/checkpoint")

    Logger.getRootLogger.setLevel(Level.WARN)

    val linesDS = ssc.actorStream[String](Props(classOf[SimpleReceiver], "akka.tcp://test@%s:%s/user/Feeder".format(args(0), args(1))), "SampleReceiver")
    val arrayDS = linesDS.map(_.trim).map(_.split("[\\s]+")).map(p => p.map(q => q.toDouble))
    val vectorDS = arrayDS.map(p => Vectors.dense(p))

    val lrModel = LogisticRegressionModel.load(sc, "lrModel")
    val bLRModel = sc.broadcast(lrModel)
    
    val predictionDS = vectorDS.transform{ rdd =>
      rdd.mapPartitions{ partition =>
        val lrModel = bLRModel.value
        partition.map(vector => lrModel.predict(vector))
      }
    }

    val predictionStringDS = predictionDS.map { behavior =>
      behavior match {
        case 0 => "WALKING"
        case 1 => "WALKING_UPSTAIRS"
        case 2 => "WALKING_DOWNSTAIRS"
        case 3 => "SITTING"
        case 4 => "STANDING"
        case 5 => "LAYING"
      }
    }

    predictionStringDS.foreachRDD{ rdd =>
      val behaviorDF = rdd.toDF("behavior")
      behaviorDF.registerTempTable("sensor_analysis")
    }

    HiveThriftServer2.startWithContext(hiveContext)

    ssc.start()
    ssc.awaitTermination()
  }
}

