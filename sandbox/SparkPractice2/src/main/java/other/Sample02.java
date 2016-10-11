package other;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hirokinaganuma on 2016/10/11.
 */
public class Sample02 {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile("bin/input/filter/*.txt");
        JavaRDD<String> result = input.filter(s-> !s.contains("Triangle"));
        result.saveAsTextFile("bin/output/output02");
        sc.stop();
    }
}
