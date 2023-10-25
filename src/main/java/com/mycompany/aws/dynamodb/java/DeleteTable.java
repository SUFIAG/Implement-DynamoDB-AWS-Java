/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;

import java.util.logging.Logger;

public final class DeleteTable {

    private static final Logger LOGGER = Logger.getLogger("DeleteTable");

    private DeleteTable(){}

    public static void deleteTable() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        try {
            LOGGER.info("Trying to delete table, please wait...");
            table.delete();
            table.waitForDelete();
            LOGGER.info("Successfully deleted table");
        } catch (Exception ex) {
            LOGGER.severe("Unable to delete table: " + table.getTableName());
            LOGGER.severe(ex.getMessage());
        }
    }
}
