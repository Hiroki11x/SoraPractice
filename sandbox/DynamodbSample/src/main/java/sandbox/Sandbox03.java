package sandbox;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * Created by hirokinaganuma on 16/11/10.
 */
public class Sandbox03 {
    public static String ACCESSKEY="access";
    public static String SECRETKEY="secret";
    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        String output;
        String dynamodbThroughputRead;
        String dynamodbThroughputReadPercent ;
        String dynamodbMaxMapTasks;

        if (args.length > 0) {
            output = args[0];
            dynamodbThroughputRead = args[1];
            dynamodbThroughputReadPercent = args[2];
            dynamodbMaxMapTasks = args[3];
        } else {
            output = "output/dynamodbresult";
            dynamodbThroughputRead = "1.0";
            dynamodbThroughputReadPercent = "1.0";
            dynamodbMaxMapTasks = "100";
            //http://stackoverflow.com/questions/10683136/amazon-elastic-mapreduce-mass-insert-from-s3-to-dynamodb-is-incredibly-slow
            //を参考にパラメータ調整
        }


        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkOnDynamoDB")
                .setMaster("local")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.rpc.askTimeout", "3000")
                .set("spark.kryoserializer.buffer","512k");

        System.setProperty("spark.storage.memoryFraction", "0.4");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JobConf jobConf = new JobConf(sc.hadoopConfiguration());
        jobConf.set("dynamodb.servicename","dynamodb");
        jobConf.set("dynamodb.input.tableName","imsiListTable");
        jobConf.set("dynamodb.endpoint","http://localhost:8000");

//        jobConf.set("dynamodb.regionid","us-east-1");//ローカルなどでセットしない

        jobConf.set("dynamodb.throughput.read",dynamodbThroughputRead);
        jobConf.set("dynamodb.throughput.read.percent",dynamodbThroughputReadPercent);
        jobConf.set("dynamodb.max.map.tasks",dynamodbMaxMapTasks);

        jobConf.set("dynamodb.awsAccessKeyId", ACCESSKEY);
        jobConf.set("dynamodb.awsSecretAccessKey", SECRETKEY);
        jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat");
        jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat");


        JavaPairRDD<Text, DynamoDBItemWritable> userInstalledApps = sc.hadoopRDD(jobConf, DynamoDBInputFormat.class, Text.class, DynamoDBItemWritable.class);

        /*
        *   def hadoopRDD[K, V, F <: InputFormat[K, V]](
                conf: JobConf,
                inputFormatClass: Class[F],
                keyClass: Class[K],
                valueClass: Class[V]
                ): JavaPairRDD[K, V] = {
                implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
                implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
                val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass)
                new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
            }
        * */

        JavaPairRDD<String, String> datas = userInstalledApps.mapToPair(new PairFunction<Tuple2<Text,DynamoDBItemWritable>, String, String>() {
            @Override
            public Tuple2<String, String> call(
                    Tuple2<Text, DynamoDBItemWritable> t)
                    throws Exception {
                Text text = t._1();
                System.out.println(text);
                System.out.println("---------HOGEHOGE-----");
                DynamoDBItemWritable item = t._2();
                Map<String, AttributeValue> attrs = item.getItem();
                AttributeValue imeiAttr = attrs.get("imsi");
                String imsi = imeiAttr.toString();
                AttributeValue appsAttr = attrs.get("operatorID");
                return new Tuple2<String, String>(imsi, appsAttr.toString());
            }
        });

//        System.out.println(datas.collect());
        datas.saveAsTextFile(output);
        long end = System.currentTimeMillis();
        System.out.println("=============================>spend time=" + (end - start) + "ms");
//        sc.stop();
    }
}
