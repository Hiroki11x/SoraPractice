package operationsamples.transition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class FilterSample4 {
    public static void main(String[] args) throws Exception {
        String inputFile = "twitter__litmon_.json";
        String outputFile = "output.md";
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory", "1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        JavaRDD<String> errorLog = input.filter(s -> s.contains("メンヘラ"));
        
        System.out.println("Search your recent 200 tweets");
        System.out.println("Number of tweets contains \"メンヘラ\": " +errorLog.count());
        System.out.println(errorLog.collect());
    }
}
