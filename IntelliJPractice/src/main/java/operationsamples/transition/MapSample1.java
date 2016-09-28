package operationsamples.transition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class MapSample1 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(-1,2,-3,4,-5));
        JavaRDD<Integer> output = input.map(i->Math.abs(i));
        System.out.println(output.collect());
    }
}
