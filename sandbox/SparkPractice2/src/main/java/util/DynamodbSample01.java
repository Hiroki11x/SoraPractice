package util;

/**
 * Created by hirokinaganuma on 2016/10/20.
 *
 *
 * Legacy
 */

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class DynamodbSample01 {


    public static void main(String[] args) throws Exception {
        String master;
        Boolean isLocal;
        if (args.length > 0) {
            master = args[0];
            isLocal = true;
        } else {
            master = "local[*]";
            isLocal = true;
        }

        SparkConf conf = new SparkConf();
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
        DynamoDB dynamo = DynamoUtils.setupDynamoClientConnection(accessKey, secretKey, isLocal);
//        DynamoUtils.createTable(dynamo);
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

        static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(
                new ProfileCredentialsProvider()));

        private static final String ACCESS_KEY_ID     = "access";
        private static final String SECRET_ACCESS_KEY = "secret";
        private static final String REGION            = "localhost";
        private static final String ENDPOINT          = "http://localhost:8000";
        private static final String TABLE_NAME = "chat";
        private static final int ROOM_ID       = 1;
        private static final int MAX_RESULT_SIZE = 10;
        private static String replyTableName = "Reply";

        @PostConstruct
        public static DynamoDB setupDynamoClientConnection(String accessKey, String secretKey, Boolean isLocal) {
            if (isLocal) {
                AWSCredentials awsCredentials = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
                AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentials);
                amazonDynamoDBClient.setEndpoint(ENDPOINT);
                return new DynamoDB(amazonDynamoDBClient);
            } else {
//                credentials = new BasicAWSCredentials(accessKey, secretKey);
//                client = new AmazonDynamoDBClient(credentials);
//                client.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
                return null;
            }
        }

        public static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits, String partitionKeyName, String partitionKeyType) {
                createTable(tableName, readCapacityUnits, writeCapacityUnits, partitionKeyName, partitionKeyType, null, null);
        }

        private static void createTable(
                String tableName, long readCapacityUnits, long writeCapacityUnits,
                String partitionKeyName, String partitionKeyType,
                String sortKeyName, String sortKeyType) {

            try {

                ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
                keySchema.add(new KeySchemaElement()
                        .withAttributeName(partitionKeyName)
                        .withKeyType(KeyType.HASH)); //Partition key

                ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
                attributeDefinitions.add(new AttributeDefinition()
                        .withAttributeName(partitionKeyName)
                        .withAttributeType(partitionKeyType));

                if (sortKeyName != null) {
                    keySchema.add(new KeySchemaElement()
                            .withAttributeName(sortKeyName)
                            .withKeyType(KeyType.RANGE)); //Sort key
                    attributeDefinitions.add(new AttributeDefinition()
                            .withAttributeName(sortKeyName)
                            .withAttributeType(sortKeyType));
                }

                CreateTableRequest request = new CreateTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(keySchema)
                        .withProvisionedThroughput( new ProvisionedThroughput()
                                .withReadCapacityUnits(readCapacityUnits)
                                .withWriteCapacityUnits(writeCapacityUnits));

                // If this is the Reply table, define a local secondary index
                if (replyTableName.equals(tableName)) {

                    attributeDefinitions.add(new AttributeDefinition()
                            .withAttributeName("PostedBy")
                            .withAttributeType("S"));

                    ArrayList<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
                    localSecondaryIndexes.add(new LocalSecondaryIndex()
                            .withIndexName("PostedBy-Index")
                            .withKeySchema(
                                    new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH),  //Partition key
                                    new KeySchemaElement() .withAttributeName("PostedBy") .withKeyType(KeyType.RANGE))  //Sort key
                            .withProjection(new Projection() .withProjectionType(ProjectionType.KEYS_ONLY)));

                    request.setLocalSecondaryIndexes(localSecondaryIndexes);
                }

                request.setAttributeDefinitions(attributeDefinitions);

                System.out.println("Issuing CreateTable request for " + tableName);
                Table table = dynamoDB.createTable(request);
                System.out.println("Waiting for " + tableName
                        + " to be created...this may take a while...");
                table.waitForActive();

            } catch (Exception e) {
                System.err.println("CreateTable request failed for " + tableName);
                System.err.println(e.getMessage());
            }
        }


        public static void registerTable(DynamoDB dynamoDB){
            Table table = dynamoDB.getTable("sample");

            //[Scala] val expressionAttributeNames = new util.HashMap[String,String]();
            HashMap<String,String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#p", "pageCount");

            //[Scala] val expressionAttributeValues = new util.HashMap[String,Object]();
            HashMap<String,Object> expressionAttributeValues = new HashMap<>();

            //[Scala] val num = 1.asInstanceOf[Object];
            Object num = (Object)1;

            expressionAttributeValues.put(":val", num);

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("Id", 120)
                    .withReturnValues(ReturnValue.ALL_NEW)
                    .withUpdateExpression("set #p = :val1")
                    .withConditionExpression("#p = :val2")
                    .withNameMap(new NameMap()
                            .with("#p", "Price"))
                    .withValueMap(new ValueMap()
                            .withNumber(":val1", 25)
                            .withNumber(":val2", 20));

            //public UpdateItemOutcome updateItem(String hashKeyName, Object hashKeyValue, String updateExpression, Map<String, String> nameMap, Map<String, Object> valueMap)
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
//                    "id",
//                    (Object)1,
//                    "set #p = #p + :val",
//                    expressionAttributeNames,
//                    expressionAttributeValues
//            );
        }
    }
}