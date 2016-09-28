package operationsamples.transition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/28.
 */
public class SpecialRDD {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        JavaDoubleRDD output = rdd.mapToDouble(
                new DoubleFunction<Integer>() {
                    @Override
                    public double call(Integer integer){
                        return (double)integer*integer;
                    }
                }
        );
        output.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(output.collect());
    }

}
