import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class FilterSample2 {
    public static void main(String[] args) throws Exception {
        String inputFile = "logsample.log";
        String outputFile = "output.md";
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        // Split up into words.
        class ContainsNotice implements Function<String,Boolean>{
            public Boolean call(String str){return str.contains("notice");}
        }
        
        JavaRDD<String> output = input.filter(new ContainsNotice());
        System.out.println(output.take(3));
    }
}
