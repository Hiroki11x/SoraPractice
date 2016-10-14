import akka.actor.Props
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Milliseconds}
import org.apache.log4j.{Level, Logger}

object StreamingThird {
  def main(args: Array[String]) {

    if(args.length != 2) {
      System.err.println(
        "Usage: StreamingThird <host> <port>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("StreamingThird")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Milliseconds(5000))
    ssc.checkpoint("/tmp/checkpoint")

    Logger.getRootLogger.setLevel(Level.WARN)

    val lines = ssc.actorStream[String](
      Props(classOf[SimpleReceiver],
        "akka.tcp://test@%s:%s/user/Feeder".format(args(0), args(1))),
      "SampleReceiver")

    val dataArray = lines.map(_.trim)
      .map(_.split("[\\s]+"))
      .map(_.take(3))
      .map(_.map(_.toDouble))

    val dataArrayInWindow = dataArray.window(Milliseconds(20000))

    val outlierInWindow = dataArrayInWindow.transform{ rdd =>
      val outlier = rdd.isEmpty match {
        case true =>
          val aryOfMax = Array(Double.MaxValue, Double.MaxValue, Double.MaxValue)
          val tmpIndex = Long.MaxValue
          rdd.sparkContext.parallelize(Array((aryOfMax, tmpIndex)))
        case _ => 
          val count = rdd.count

          val sum = rdd.reduce{ (a: Array[Double], b: Array[Double]) =>
            (0 until a.size).map(i => a(i) + b(i)).toArray
          }

          val mean = (0 until sum.size).map(i => sum(i)/count).toArray

          val deviation = rdd.map{ p =>
            (0 until p.size).map{ i =>
              scala.math.pow(p(i) - mean(i), 2)
            }.toArray
          }

          val vr = deviation.reduce{ (a: Array[Double], b: Array[Double]) =>
            (0 until a.size).map(i => a(i) + b(i)).toArray
          }.map(p => p / count)

          val stdDev = vr.map(p => scala.math.sqrt(p))

          val dfMnWithIndex = rdd.zipWithIndex.map{ case (values, index) =>
            val dfMn = (0 until values.size).map{ i =>
              math.abs((values(i) - mean(i)))
            }.toArray
            (dfMn, index)
          }

          val otl = dfMnWithIndex.filter{ case (values, index) =>
            (0 until values.size).exists(i => values(i) > 3 * stdDev(i))
          }

          otl
      }

      outlier.map { case (values, index) =>
        (values.mkString(","), index )
      }
    }

    

    outlierInWindow.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

