/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import java.util.logging.Logger;

public class Scan {

    private static final Logger LOGGER = Logger.getLogger("Scan");

    public static void scan() {
        DynamoDB dynamoDB = AmazonDynamoDBClientManager.getDynamoDB();
        Table table = dynamoDB.getTable("Movies");

        ScanSpec scanSpec = new ScanSpec()
            .withProjectionExpression("#yr, title, info.rating")
            .withFilterExpression("#yr between :start_year and :end_year")
            .withNameMap(new NameMap().with("#yr", "year"))
            .withValueMap(new ValueMap().withNumber(":start_year", 1950).withNumber(":end_year", 1959));

        try {
            ItemCollection<ScanOutcome> items = table.scan(scanSpec);

            for (Item item : items) {
                LOGGER.info(item.toString());
            }

        } catch (Exception ex) {
            LOGGER.severe("Unable to scan movie");
            LOGGER.severe(ex.getMessage());
        }
    }
}
