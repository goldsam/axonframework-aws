package com.github.goldsam.axonframework.aws.eventstore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

/**
 *
 * @author samgo
 */
public class Scan implements RequestHandler<DynamodbEvent, Void> {

    private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
    
    @Override
    public Void handleRequest(DynamodbEvent event, Context context) {
        event.
        event.getRecords().get(0).getDynamodb().get
        
        return null;
    }    
}
