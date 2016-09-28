package localelementarysamples;

import akka.japi.Function2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/28.
 */
public class CombineByKeySample1 {

    public static class AvgCount implements Serializable{
        public int total,num;
        public  AvgCount(int total,int num){
            this.total = total;
            this.num = num;
        }
        public float avg(){
            return total/(float)num;
        }
    }

    Function<Integer,AvgCount> createAcc = new Function<Integer, AvgCount>() {
        @Override
        public AvgCount call(Integer integer) throws Exception {
            return new AvgCount(integer,1);
        }
    };

    Function2<AvgCount,Integer,AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
        @Override
        public AvgCount apply(AvgCount avgCount, Integer integer) throws Exception {
            avgCount.total += integer;
            avgCount.num ++;
            return avgCount;
        }
    };

    Function2<AvgCount,AvgCount,AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
        @Override
        public AvgCount apply(AvgCount avgCount, AvgCount avgCount2) throws Exception {
            avgCount.total += avgCount2.total;
            avgCount.num += avgCount2.num;
            return avgCount;
        }
    };

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
        );
        System.out.println(result.collect());
    }

}
