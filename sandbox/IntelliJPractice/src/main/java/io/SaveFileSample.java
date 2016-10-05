package io;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hirokinaganuma on 2016/09/29.
 */
public class SaveFileSample {
    public static void main(String[] args) throws Exception {
        String inputFile = "bin/data/*.json";//まとめて読み込むことも可能
        String outputPath = "result/output00";
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> output = input.filter(s -> s.contains("\"text\":"))
                .map(s->s.replaceAll("#GTCJapan",""))
                .map(s->s.replaceAll("#GTCjapan",""))
                .map(s->s.replaceAll("#gtcjapan",""))
                .map(s->s.replaceAll("\"text\":",""))
                .map(s->s.replaceAll("\"",""))
                .map(s->s.replaceAll(",",""))
                .distinct();
        output.saveAsTextFile(outputPath);
        System.out.println(output);
        sc.stop();
    }
}
