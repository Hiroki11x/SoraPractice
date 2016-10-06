package io;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hirokinaganuma on 2016/10/06.
 */
public class ActivityLogSample01 {
    public static void main(String[] args) throws Exception {
        String inputFile = "../../../ActivityLog/*.md";//まとめて読み込むことも可能
        String outputPath = "result/output05";
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }

        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile(inputFile);
        //System.out.println("hoge");みたいなのが何回呼ばれるか調べる
        JavaRDD<String> output = input.filter(s->s.contains("Spark")).distinct();
        output.saveAsTextFile(outputPath);
        System.out.println(output);
        sc.stop();
    }
}
