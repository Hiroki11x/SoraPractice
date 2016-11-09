package sandbox;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 16/11/09.
 */
public class Sandbox01 {
    public static void main(String[] args) throws Exception {
        String master;
        if (args.length > 0) {
            master = args[0];
        } else {
            master = "local[*]";
        }
        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        JavaRDD<String> imsiList = sc.textFile("bin/input/Dummydata/imsi-list.txt");//imsiとオペレーターIDの関連付けのリスト
//        JavaRDD<String> operatorList = sc.textFile("bin/input/Dummydata/operator-list.txt");//オペレーターID一覧のリスト
//        JavaRDD<String> imsiItemList = sc.textFile("bin/input/Dummydata/imsi-item-list.txt");//imsiとそれに紐づく課金（ITEM-1からITEM-3とそれぞれの費用）が入っている

        AmazonDynamoDBClient client = new AmazonDynamoDBClient()
                .withEndpoint("http://localhost:8000");

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("imisListTable");

        if (table==null){
            table = createTable(dynamoDB);
        }
        registerRecords(table,imsiList);
    }

    public static Table createTable(DynamoDB dynamoDB) {
        String tableName = "imsiListTable";
        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(
                            new KeySchemaElement("imsi", KeyType.HASH),  //Partition key
                            new KeySchemaElement("operatorID", KeyType.RANGE)), //Sort key
                    Arrays.asList(
                            new AttributeDefinition("imsi", ScalarAttributeType.N),//数字として
                            new AttributeDefinition("operatorID", ScalarAttributeType.S)),//文字列として
                    new ProvisionedThroughput(10L, 10L));//TODO これよくわかってない
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());
            return table;
        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }
        return null;
    }

    public static void registerRecords(Table table,JavaRDD<String> imsiList) {
        imsiList.foreach(s -> {//TODO Task not serializable(https://github.com/Hiroki11x/SoraPractice/issues/9)
            long imsi = Long.valueOf(s.split(",")[0]);
            String operatorID = s.split(",")[1];

            try {
                System.out.println("Adding a new item...");
                PutItemOutcome outcome = table.putItem(new Item()//TODO tableはSparkのクロージャからはアクセスできても書き込みが反映されないのでは？
                        .withPrimaryKey("imsi", imsi, "operatorID", operatorID));
                System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

            } catch (Exception e) {
                System.err.println("Unable to add item: " + imsi + " " + operatorID);
                System.err.println(e.getMessage());
            }
        });
    }
}
