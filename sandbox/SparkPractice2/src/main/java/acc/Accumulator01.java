package acc;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/10/01.
 */
public class Accumulator01 {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> rdd = sc.textFile("bin/input/README.md");


        final Accumulator<Integer> blankLines = sc.accumulator(0);
        JavaRDD<String> callSigns = rdd.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        if(s.equals("")){
                            blankLines.add(1);
                        }
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        callSigns.saveAsTextFile("bin/output/output.txt");
        System.out.println("Blank lines: "+blankLines.value());
    }
}
