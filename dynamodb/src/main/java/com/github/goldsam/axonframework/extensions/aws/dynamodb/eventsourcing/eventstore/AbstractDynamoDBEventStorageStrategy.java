package com.github.goldsam.axonframework.extensions.aws.dynamodb.eventsourcing.eventstore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventhandling.StreamNameResolver;
import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventsourcing.eventstore.itemperevent.EventEntryConfiguration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.axonframework.common.ObjectUtils.getOrDefault;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.Serializer;

/**
 * AbstractKinesisEventStorageStrategy
 */
public abstract class AbstractDynamoDBEventStorageStrategy implements DynamoDBStorageStrategy {
    
    private final EventEntryConfiguration eventConfiguration; 

    public AbstractDynamoDBEventStorageStrategy(EventEntryConfiguration eventConfiguration) {
        this.eventConfiguration = getOrDefault(eventConfiguration, EventEntryConfiguration.getDefault());
    }
    
    @Override
    public void appendEvents(AmazonDynamoDB dynamoDB, String tableName, List<? extends EventMessage<?>> events, Serializer serializer) {
        new Item()
                .withList(tableName, vals)
        dynamoDB.
        new PutItemRequest()
                .withTableName(tableName)
                .with
        
        new BatchWriteItemRequest()
                .addRequestItemsEntry(key, value)
        createEventItems(events, serializer)
        
    }
    
    /**
     * Returns a stream of DynamoDB items that represent the given batch of events. The given list of {@code events}
     * represents events produced in the context of a single unit of work. Uses the given {@code serializer} to
     * serialize event payload and metadata.
     *
     * @param events     the events to convert to DynamoDB items.
     * @param serializer the serializer to convert the events' payload and metadata
     * @return stream of DynamoDB items from the given event batch
     */
    protected abstract Stream<Item> createEventItems(List<? extends EventMessage<?>> events, Serializer serializer);

    /**
     * Returns the {@link EventEntryConfiguration} that configures how 
     * event entries are mapped to DynamoDB items.
     *
     * @return the event entry configuration
     */
    protected EventEntryConfiguration eventEntryConfiguration() {
        return eventConfiguration;
    }
}
