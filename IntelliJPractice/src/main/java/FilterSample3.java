import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Created by hirokinaganuma on 2016/09/27.
 */
public class FilterSample3 {
    public static void main(String[] args) throws Exception {
        String inputFile = "logsample.log";
        String outputFile = "output.md";
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]").set("spark.executor.memory","1g");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);
        class Contains implements Function<String,Boolean>{
            private String argQuery;
            public Contains(String arg){this.argQuery = arg;}
            public Boolean call(String str){return str.contains(argQuery);}
        }
        JavaRDD<String> errorLog = input.filter(new Contains("notice"));
        System.out.println(errorLog.take(3));
    }
}
