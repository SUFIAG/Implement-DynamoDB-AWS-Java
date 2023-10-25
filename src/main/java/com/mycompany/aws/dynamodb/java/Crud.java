/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public final class Crud {

    private static final Logger LOGGER = Logger.getLogger("Crud");

    private Crud(){}

    public static void createNewItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        final Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("plot", "Nothing happens at all.");
        infoMap.put("rating", 0);

        Item item = new Item()
            .withPrimaryKey("year", year, "title", title)
            .withMap("info", infoMap);

        try {
            PutItemOutcome putItemOutcome = table.putItem(item);
            LOGGER.info("PutItem succeeded:\n" + putItemOutcome.getPutItemResult());
        } catch (Exception ex) {
            LOGGER.severe("Unable to add movie: " + year + " " + title);
            LOGGER.severe(ex.getMessage());
        }
    }

    public static void readItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        GetItemSpec spec = new GetItemSpec()
            .withPrimaryKey("year", year, "title", title);

        try {
            Item item = table.getItem(spec);
            LOGGER.info("{} +  item" + item);
        } catch (Exception ex) {
            LOGGER.severe("Unable to read item: " + year + " " + title);
            LOGGER.severe(ex.getMessage());
        }
    }

    public static void updateItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        ValueMap valueMap = new ValueMap()
            .withNumber(":r", 5.5)
            .withString(":p", "Everything happens all at once")
            .withList(":a", Arrays.asList("Harry", "James", "Danny"));

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
            .withPrimaryKey("year", year, "title", title)
            .withUpdateExpression("set info.rating = :r, info.plot = :p, info.actors = :a")
            .withValueMap(valueMap)
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            LOGGER.info("Updating item: " + updateItemSpec);
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            LOGGER.info("UpdateItem succeeded:\n" + updateItemOutcome.getItem().toJSONPretty());
        } catch (Exception ex) {
            LOGGER.severe("Unable to update movie: " + year + " " + title);
            LOGGER.severe(ex.getMessage());
        }
    }

    public static void incrementAtomicCounters() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
            .withPrimaryKey("year", year, "title", title)
            .withUpdateExpression("set info.rating = info.rating + :val")
            .withValueMap(new ValueMap().withNumber(":val", 1))
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            LOGGER.info("Incrementing an atomic counter...");
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            LOGGER.info("UpdateItem succeeded:\n" + updateItemOutcome.getItem().toJSONPretty());
        } catch (Exception ex) {
            LOGGER.severe("Unable to update movie: " + year + " " + title);
            LOGGER.severe(ex.getMessage());
        }
    }

    public static void conditionalUpdateItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
            .withPrimaryKey(new PrimaryKey("year", year, "title", title))
            .withUpdateExpression("remove info.actors[0")
            .withConditionExpression("size(info.actors) > :num")
            .withValueMap(new ValueMap().withNumber(":val", 3))
            .withReturnValues(ReturnValue.UPDATED_NEW);

        try {
            LOGGER.info("Going for a conditional update");
            UpdateItemOutcome updateItemOutcome = table.updateItem(updateItemSpec);
            LOGGER.info("UpdateItem succeeded:\n" + updateItemOutcome.getItem().toJSONPretty());
        } catch (Exception ex) {
            LOGGER.severe("Unable to update movie: " + year + " " + title);
            LOGGER.severe(ex.getMessage());
        }
    }

    public static void deleteItem() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        int year = 2015;
        String title = "The Big New Movie";

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
            .withPrimaryKey(new PrimaryKey("year", year, "title", title))
            .withConditionExpression("info.rating <= :val")
            .withValueMap(new ValueMap().withNumber(":val", 5.0));

        try {
            LOGGER.info("Attempting to conditionally delete an item...");
            table.deleteItem(deleteItemSpec);
            LOGGER.info("Deleted item successfully");
        } catch (Exception ex) {
            LOGGER.severe("Unable to delete movie: " + year + " " + title);
            LOGGER.severe(ex.getMessage());
        }
    }
}
