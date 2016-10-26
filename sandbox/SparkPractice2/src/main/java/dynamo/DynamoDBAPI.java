package dynamo;

/**
 * Created by hirokinaganuma on 2016/10/26.
 */
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamoDBAPI {

    private final static Logger logger = LoggerFactory.getLogger(DynamoDBAPI.class);

    private final String ACCESS_KEY_ID     = "access";
    private final String SECRET_ACCESS_KEY = "secret";
    private final String REGION            = "localhost";
    private final String ENDPOINT          = "http://localhost:8000";
    private static final String TABLE_NAME = "chat";
    private static final int ROOM_ID       = 1;
    private static final int MAX_RESULT_SIZE = 10;

    private DynamoDB dynamoDB;

    @PostConstruct
    private void init(){

        AWSCredentials awsCredentials = new BasicAWSCredentials(ACCESS_KEY_ID, SECRET_ACCESS_KEY);
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentials);
        amazonDynamoDBClient.setEndpoint(ENDPOINT);

        dynamoDB = new DynamoDB(amazonDynamoDBClient);
    }

    public void put(DBItem DBItem) {

        Item item = new Item()
                .withPrimaryKey("room_id", ROOM_ID)
                .withString("commented_datetime", String.valueOf(DBItem.getCommented_datetime()))
                .withString("user_name", DBItem.getUserName())
                .withString("message", DBItem.getMessage());

        dynamoDB.getTable(TABLE_NAME).putItem(item);
    }

    public List<DBItem> getDBItems(){

        QuerySpec spec = new QuerySpec()
                .withHashKey("room_id", ROOM_ID)
                .withScanIndexForward(false)
                .withMaxResultSize(MAX_RESULT_SIZE);

        ItemCollection<QueryOutcome>items = dynamoDB.getTable(TABLE_NAME).query(spec);
        List<DBItem>DBItems = new ArrayList<DBItem>();
        for (Iterator<Item> iterator = items.iterator(); iterator.hasNext();) {
            Item item = iterator.next();
            DBItems.add(copy(item));
        }

        return DBItems;
    }

    public List<DBItem> getAll(){

        QuerySpec spec = new QuerySpec()
                .withHashKey("room_id", ROOM_ID)
                .withScanIndexForward(false);

        ItemCollection<QueryOutcome>items = dynamoDB.getTable(TABLE_NAME).query(spec);
        List<DBItem>DBItems = new ArrayList<DBItem>();
        for (Iterator<Item> iterator = items.iterator(); iterator.hasNext();) {
            Item item = iterator.next();
            DBItems.add(copy(item));
        }

        return DBItems;
    }

    private DBItem copy(Item item){

        DBItem DBItem = new DBItem();
        DBItem.setUserName(item.getString("user_name"));
        DBItem.setCommented_datetime(Long.parseLong(item.getString("commented_datetime")));
        DBItem.setMessage(item.getString("message"));

        return DBItem;
    }

    public void createTable() throws Exception {

        List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("room_id")
                .withAttributeType(ScalarAttributeType.N));
        attributeDefinitions.add(new AttributeDefinition()
                .withAttributeName("commented_datetime")
                .withAttributeType(ScalarAttributeType.S));

        List<KeySchemaElement> keySchemaElements = new ArrayList<KeySchemaElement>();
        keySchemaElements.add(new KeySchemaElement()
                .withAttributeName("room_id")
                .withKeyType(KeyType.HASH));
        keySchemaElements.add(new KeySchemaElement()
                .withAttributeName("commented_datetime")
                .withKeyType(KeyType.RANGE));

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(5L)
                .withWriteCapacityUnits(6L);

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withProvisionedThroughput(provisionedThroughput);

        Table table = dynamoDB.createTable(request);

        try {
            table.waitForActive();

        } catch (InterruptedException e) {

            logger.error("failed create table={}", e);
            throw new Exception();
        }

        getTableInformation();

    }

    public void deleteTable(){

        Table table = dynamoDB.getTable(TABLE_NAME);
        table.delete();
    }

    public void getTableInformation() {

        TableDescription tableDescription = dynamoDB.getTable(TABLE_NAME).describe();

        logger.debug("TableName:{}", tableDescription.getTableName());
        logger.debug("Status:{}", tableDescription.getTableStatus());
        logger.debug("Provisioned Throughput (read capacity units/sec):{}",
                tableDescription.getProvisionedThroughput().getReadCapacityUnits());
        logger.debug("Provisioned Throughput (write capacity units/sec):{}",
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }
}