package com.github.goldsam.axonframework.extensions.aws.kinesis.eventsourcing.eventstore;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.github.goldsam.axonframework.extensions.aws.kinesis.eventhandling.StreamNameResolver;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.axonframework.common.ObjectUtils.getOrDefault;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.Serializer;

/**
 * AbstractKinesisEventStorageStrategy
 */
public abstract class AbstractKinesisEventStorageStrategy implements KinesisStorageStrategy {
    
    private final KinesisEventStorageConfiguration eventStorageConfiguration;

    public AbstractKinesisEventStorageStrategy(KinesisEventStorageConfiguration eventStorageConfiguration) {
        this.eventStorageConfiguration = getOrDefault(eventStorageConfiguration, KinesisEventStorageConfiguration.getDefault());
    }
    
    @Override
    public void appendEvents(AmazonKinesis kinesisClient, List<? extends EventMessage<?>> events, Serializer serializer) {
        final StreamNameResolver streamNameResolver = eventStorageConfiguration().streamNameResolver();
        events.stream()
                .collect(Collectors.groupingBy(event -> streamNameResolver.resolveStreamName(event)))
                .forEach((streamName, streamEvents) -> appendEvents(kinesisClient, streamName, streamEvents, serializer));
    }
   
    /**
     * Sends the given list of {@code events} to the specific Kinesis {@code streamName} 
     * using the given {@code kinesisClient}. 
     * Uses the given {@code serializer} to serialize the payload and metadata of the events.
     * 
     * @param kinesisClient     Amazon Kinesis client.
     * @param streamName        Target Kinesis stream name.
     * @param events            the event messages to append to the "put" request.
     * @param serializer        the serializer used to serialize the event payload and metadata
     */
    protected abstract void appendEvents(AmazonKinesis kinesisClient, String streamName, List<? extends EventMessage<?>> events, Serializer serializer);
    
    /**
     * Returns a stream of Kinesis "put" request entries that represent the given batch of events. 
     * The given list of {@code events} represents events produced in the context of a single Unit of Work. 
     * Uses the given {@code serializer} to serialize event payload and metadata.
     *
     * @param k
     * @param events     the events to convert to Kinesis "put" request entries.
     * @param serializer the serializer to convert the events' payload and metadata
     * @return stream of Kinesis "put" record entries for the given event batch
     */
//    protected abstract Stream<PutRecordsRequestEntry> createPutRequestEntries(List<? extends EventMessage<?>> events, Serializer serializer);

    /**
     * Returns the {@link KinesisEventStorageConfiguration} that configures how 
     * event entries are mapped to a Kinesis stream.
     *
     * @return the event entry configuration
     */
    protected KinesisEventStorageConfiguration eventStorageConfiguration() {
        return eventStorageConfiguration;
    }
}
