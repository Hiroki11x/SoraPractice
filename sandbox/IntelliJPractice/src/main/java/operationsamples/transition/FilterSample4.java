package operationsamples.transition;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class FilterSample4 {
    public static void main(String[] args) throws Exception {
        String inputFile = "bin/data/*.json";//まとめて読み込むことも可能
        String outputFile = "output01";
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> output = input.filter(s -> s.contains("\"text\":")).filter(s -> s.contains("笑"));
        output.saveAsTextFile("result/output01");
        sc.stop();
    }
}
