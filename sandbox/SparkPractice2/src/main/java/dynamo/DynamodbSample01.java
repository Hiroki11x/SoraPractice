package dynamo;

/**
 * Created by hirokinaganuma on 2016/10/20.
 */
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by hirokinaganuma on 2016/10/14.
 */
public class DynamodbSample01 {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local[*]";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));

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
        DynamoUtils.createTable(dynamo);
        DynamoUtils.registerTable(dynamo);

        System.out.println("----Start----");

        rdd.map(v -> {
            Table table = dynamo.getTable("sample");
            PrimaryKey key = new PrimaryKey("id", 1);
            Item item = table.getItem(key);
            return item.toString();
        }).saveAsTextFile(file);
    }


    static class DynamoUtils {

        static public DynamoDB setupDynamoClientConnection(String accessKey, String secretKey) {

            BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentials);
            client.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));

            return new DynamoDB(client);
        }

        static public void createTable(DynamoDB dynamoDB){
            String tableName = "sample";
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName("HogeId").withKeyType(KeyType.HASH));
            AttributeDefinition attrDef = new AttributeDefinition().withAttributeName("HogeId").withAttributeType(ScalarAttributeType.S);
            ProvisionedThroughput pt = new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L);
            dynamoDB.createTable(new CreateTableRequest(tableName, keySchema).withAttributeDefinitions(attrDef).withProvisionedThroughput(pt));
        }


        static public void registerTable(DynamoDB dynamoDB){
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
        }
    }
}