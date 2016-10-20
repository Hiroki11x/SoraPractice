
package demo;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by hirokinaganuma on 2016/10/14.
 */
public class Sample03 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-1,2,-3,4,-5));
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        String file;
        if (args.length > 0) {
            file = args[0];
        } else {
            file = "outputfile";
        }

        //scalaではlazyとして遅延評価されていた
        DynamoDB dynamo = DynamoUtils.setupDynamoClientConnection(accessKey, secretKey);

        System.out.println("----Start----");
        rdd.map(v -> {
                Table table = dynamo.getTable("sample");
                PrimaryKey key = new PrimaryKey("id", 1);
                Item ss = table.getItem(key);
                return ss.toString();
        }).saveAsTextFile(file);
    }

    
    static class DynamoUtils {

        static public DynamoDB setupDynamoClientConnection(String accessKey, String secretKey){
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
            client.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
            DynamoDB dynamoDB = new DynamoDB(client);

            Table table = dynamoDB.getTable("sample");


            //[Scala] val expressionAttributeNames = new util.HashMap[String,String]();
            HashMap<String,String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#p", "pageCount");

            //[Scala] val expressionAttributeValues = new util.HashMap[String,Object]();
            HashMap<String,Object> expressionAttributeValues = new HashMap<>();

            //[Scala] val num = 1.asInstanceOf[Object];
            Object num = (Object)1;

            expressionAttributeValues.put(":val", num);

            UpdateItemOutcome outcome = table.updateItem(
                "id", 1,
                "set #p = #p + :val",
                expressionAttributeNames,
                expressionAttributeValues);
                return dynamoDB;
            }

    }
    
}
