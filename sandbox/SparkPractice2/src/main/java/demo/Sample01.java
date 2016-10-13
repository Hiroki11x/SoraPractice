package demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by hirokinaganuma on 2016/10/13.
 */
public class Sample01 {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        JavaRDD<String> imsiList = sc.textFile("bin/input/Dummydata/imsi-list.txt");//imsiとオペレーターIDの関連付けのリスト
        JavaRDD<String> operatorList = sc.textFile("bin/input/Dummydata/operator-list.txt");//オペレーターID一覧のリスト
        JavaRDD<String> imsiItemList = sc.textFile("bin/input/Dummydata/imsi-item-list.txt");//imsiとそれに紐づく課金（ITEM-1からITEM-3とそれぞれの費用）が入っている

        JavaPairRDD<String,String> imsiPairRDD = imsiList.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String x) {
                        return new Tuple2(x.split(",")[1], x.split(",")[0]);
                    }
                }
        );//imsiPairRDDはオペレータ−IDがkey,imsi番号がvalue
//        imsiItemList.cache();

        JavaPairRDD<String,Integer> itemPairRDD = imsiItemList.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x.split(",")[0], x.split(",")[2]);
                    }
                }
        );//imsiPairRDDはオペレータ−IDがkey,課金額がvalue
//        itemPairRDD.cache();

        JavaPairRDD<String,Integer> itemReducedPairRDD = itemPairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer+integer2;
                    }
                }
        );//imsiごとの集計結果のLIST,imsiがkey,合計か金額がvalue
        itemReducedPairRDD.cache();


//        imsiPairRDD.saveAsTextFile("bin/output/imsiPairRDD");
//        itemPairRDD.saveAsTextFile("bin/output/itemPairRDD");
        itemPairRDD.saveAsTextFile("bin/output/itemReducedPairRDD");

        ////最終出力としては、オペレーターIDごとに、もっているIMSIのITEM1から3までの合計が出る形
    }
}
