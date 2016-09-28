package localelementarysamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/28.
 */
public class RDDPairSample1 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[1]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("README.md");
        JavaRDD<String> words = rdd.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Arrays.asList(x.split(" "));
                    }
                }
        );

        JavaPairRDD<String, Integer> result = words.mapToPair(
                new PairFunction<String, String, Integer>(){
                    public Tuple2<String, Integer> call(String x){
                        return new Tuple2(x, 1);
                    }
                }
        ).reduceByKey(new Function2<Integer, Integer, Integer>(){
            public Integer call(Integer x, Integer y){ return x+y;
            }
        });

        System.out.println(result.collect());
    }
}
