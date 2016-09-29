package localelementarysamples;

/**
 * Created by hirokinaganuma on 2016/09/28.
 */

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
public class CombineByKeySample2 {

    public static class AvgCount implements java.io.Serializable {
        public AvgCount(int total, int num) {
            total_ = total;
            num_ = num;
        }
        public int total_;
        public int num_;
        public float avg() {
            return total_ / (float) num_;
        }
    }
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        JavaSparkContext sc = new JavaSparkContext(
                master, "PerKeyAvg", System.getenv("SPARK_HOME"), System.getenv("JARS"));


        List<Tuple2<String, Integer>> input = new ArrayList();
        input.add(new Tuple2("coffee", 1));
        input.add(new Tuple2("coffee", 2));
        input.add(new Tuple2("pandas", 3));

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
        //RDD生成でデータロードじゃなくPairRDDを分散させる際はparallelizeじゃなくてparallelizepairsを使う

        /*
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
        */

        //そのパーティション内で初めて見たkeyの場合アキュムレーターの初期値を生成
        Function<Integer, AvgCount> createAcc = new Function<Integer, AvgCount>() {
            @Override
            public AvgCount call(Integer x) {
                return new AvgCount(x, 1);
            }
        };

        //パーティション内部で同じkeyのものを加算していく
        Function2<AvgCount, Integer, AvgCount> addAndCount = new Function2<AvgCount, Integer, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, Integer x) {
                a.total_ += x;
                a.num_ += 1;
                return a;
            }
        };

        //違うパーティション同士で足しあわせてまとめる
        Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount a, AvgCount b) {
                a.total_ += b.total_;
                a.num_ += b.num_;
                return a;
            }
        };
        AvgCount initial = new AvgCount(0,0);
        JavaPairRDD<String, AvgCount> avgCounts = rdd.combineByKey(createAcc, addAndCount, combine);
        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Entry<String, AvgCount> entry : countMap.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue().avg());
        }
    }
}