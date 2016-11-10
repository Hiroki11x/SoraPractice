package sandbox;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by hirokinaganuma on 16/11/10.
 */
public class Sandbox02 {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local[0]";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> imsiList = sc.textFile("bin/input/Dummydata/imsi-list.txt");//imsiとオペレーターIDの関連付けのリスト

        AmazonDynamoDBClient client = new AmazonDynamoDBClient()
                .withEndpoint("http://localhost:8000");

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("imisListTable");



        //TODO WIP
        //spark-dynamodbライブラリを使ってみたい
    }
}
