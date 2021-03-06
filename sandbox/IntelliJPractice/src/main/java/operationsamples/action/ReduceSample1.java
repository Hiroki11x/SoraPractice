package operationsamples.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class ReduceSample1 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        Integer Result = input.reduce((a, b) -> (a + b));
        System.out.println(Result);
    }
}
