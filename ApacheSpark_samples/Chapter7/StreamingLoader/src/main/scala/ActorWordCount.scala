import akka.actor.{Actor, Props}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.receiver.ActorHelper
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag


class SampleActorReceiver[T: ClassTag](urlOfPublisher: String)
  extends Actor with ActorHelper {

  lazy private val remotePublisher = context.actorSelection(urlOfPublisher)

  override def preStart(): Unit = remotePublisher ! SubscribeReceiver(context.self)

  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = remotePublisher ! UnsubscribeReceiver(context.self)

}

object ActorWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: ActorWordCount <hostname> <port>")
      System.exit(1)
    }

    val Seq(host, port) = args.toSeq
    val sparkConf = new SparkConf().setAppName("ActorWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = ssc.actorStream[String](
      Props(new SimpleReceiver("akka.tcp://test@%s:%s/user/Feeder".format(
        host, port.toInt))), "SampleReceiver")

    lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
