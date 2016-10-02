package operationsamples.transition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by hirokinaganuma on 2016/09/28.
 *
 * タプルとは、データベースやファイルでいう1行・1レコードのデータのようなもの。
 * つまり複数のデータを1つの塊として扱える。
 * タプルを使って、メソッド・関数から複数の値を一度に返すことが出来るので、非常に便利。
 *
 */
public class PairRDDSample1 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[1]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("README.md");

        PairFunction<String,String,String> keyData = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0],s);
            }
        };

        JavaPairRDD<String,String> output = input.mapToPair(keyData);
        System.out.println(output.collect());
    }
}
