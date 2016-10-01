package other; /**
 * Created by hirokinaganuma on 2016/10/01.
 */
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/09/27.
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
        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        Integer Result = input.reduce((a, b) -> (a + b));
        System.out.println(Result);
    }
}