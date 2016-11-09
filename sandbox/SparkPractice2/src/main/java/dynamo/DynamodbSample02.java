package dynamo;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by hirokinaganuma on 16/10/26.
 */

public class DynamodbSample02 {


    public static void main(String[] args) throws Exception {

        String master;
        Boolean isLocal;
        if (args.length > 0) {
            isLocal = true;
            master = args[0];

        } else {
            isLocal = true;
            master = "local[*]";
        }

        JavaSparkContext sc = new JavaSparkContext(master, "basicavg", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(-1, 2, -3, 4, -5));

        //remove comment out when using remote dyanmodb
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        String file;
        if (args.length > 0) {
            file = args[0];
        } else {
            file = "outputfile";
        }

        //Dynamodb操作

        /*
        rdd.map(v -> {
            Table table = dynamoDB.getTable("movie");
            PrimaryKey key = new PrimaryKey("year", 1);
            Item item = table.getItem(key);
            return item.toString();
        }).saveAsTextFile(file);
        */
    }

    static class DynamoUtils {

        static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(
                new ProfileCredentialsProvider()));

        private static final String ACCESS_KEY_ID = "access";
        private static final String SECRET_ACCESS_KEY = "secret";
        private static final String REGION = "localhost";
        private static final String ENDPOINT = "http://localhost:8000";
        private static final String TABLE_NAME = "chat";
        private static final int ROOM_ID = 1;
        private static final int MAX_RESULT_SIZE = 10;
        private static String replyTableName = "Reply";

    }
}