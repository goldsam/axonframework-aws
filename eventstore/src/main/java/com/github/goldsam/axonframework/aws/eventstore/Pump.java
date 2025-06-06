package com.github.goldsam.axonframework.aws.eventstore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.util.DateUtils;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis-create-package.html#with-kinesis-example-deployment-pkg-java
 * 
 */
public class Pump implements RequestHandler<KinesisEvent, Void> {
    private static final String BUFFER_TABLE_ENV_VAR = "DYNAMO_BUFFER_TABLE";
    
    private static final Charset CHARSET = Charset.forName("UTF-8");
    
    private final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
    private final MessageDigest messageDigest = createMessageDigest();
    
    @Override
    public Void handleRequest(KinesisEvent event, Context context) {
        event.getRecords().forEach(record -> {
            client.putItem(new PutItemRequest()
                    .withTableName(System.getenv(BUFFER_TABLE_ENV_VAR))
                    .addItemEntry("partitionKey", new AttributeValue().withB(ByteBuffer.wrap(
                            messageDigest.digest((record.getEventSourceARN() + record.getEventID()).getBytes(CHARSET)))))
                    .addItemEntry("eventId", new AttributeValue().withS(record.getEventID()))
                    .addItemEntry("eventTimeStamp", new AttributeValue().withS(DateUtils.formatISO8601Date(
                            record.getKinesis().getApproximateArrivalTimestamp())))
                    .addItemEntry("eventPayload", new AttributeValue().withB(record.getKinesis().getData())));
        });
      
        return null;
    }
    
    private static MessageDigest createMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch(NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to create message digrest: " + ex.getMessage(), ex);
        }
    }
}
