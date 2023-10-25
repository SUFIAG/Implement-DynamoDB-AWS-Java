/*
 * |-------------------------------------------------
 * | Copyright © 2019 Colin But. All rights reserved.
 * |-------------------------------------------------
 */
package com.mycompany.aws.dynamodb.java;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public final class AmazonDynamoDBClientManager {

    private static final String DYNAMODB_ENDPOINT = "http://localhost:8080";
    private static final String REGION = "us-west-2";

    private static DynamoDB dynamoDB;

    static {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(DYNAMODB_ENDPOINT, REGION))
            .build();

        dynamoDB = new DynamoDB(client);
    }

    private AmazonDynamoDBClientManager(){}

    static DynamoDB getDynamoDB() {
        return dynamoDB;
    }
}
