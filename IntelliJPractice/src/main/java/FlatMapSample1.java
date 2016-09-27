import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class FlatMapSample1 {


    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.parallelize(Arrays.asList("Hello I am a dog"));
        JavaRDD<String> output = input.flatMap(s -> Arrays.asList(s.split(" ")));

        /*
        JavaRDD<String> output = input.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        */
        System.out.println(output.collect());
    }
}
