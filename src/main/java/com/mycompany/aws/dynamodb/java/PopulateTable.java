/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

public final class PopulateTable {

    private static final Logger LOGGER = Logger.getLogger("PopulateTable");

    private PopulateTable(){}

    public static void loadData() throws IOException {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        JsonParser jsonParser = new JsonFactory().createParser(new File("moviedata.json"));

        JsonNode rootNode = new ObjectMapper().readTree(jsonParser);
        Iterator<JsonNode> iterator = rootNode.iterator();

        ObjectNode currentNode;

        while (iterator.hasNext()) {
            currentNode = (ObjectNode) iterator.next();

            int year = currentNode.path("year").asInt();
            String title = currentNode.path("title").asText();

            Item item = new Item()
                .withPrimaryKey("year", year, "title", title)
                .withJSON("info", currentNode.path("info")
                    .toString());

            try {
                table.putItem(item);
                LOGGER.info(String.format("PutItem succeeded: %d %s", year, title));
            } catch (Exception ex) {
                LOGGER.severe("Unable to add movie: " + year + " " + title);
                LOGGER.severe(ex.getMessage());
            }
        }
    }
}
