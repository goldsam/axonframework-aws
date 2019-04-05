package com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.Serializer;

import static org.axonframework.common.ObjectUtils.getOrDefault;

/**
 * AbstractKinesisEventStorageStrategy
 */
public abstract class AbstractKinesisEventStorageStrategy implements KinesisEventStorageStrategy {

    private final KinesisEventStorageConfiguration eventStorageConfiguration;

    public AbstractKinesisEventStorageStrategy(KinesisEventStorageConfiguration eventStorageConfiguration) {
        this.eventStorageConfiguration = getOrDefault(eventStorageConfiguration, KinesisEventStorageConfiguration.getDefault());
    }

    @Override
    public PutRecordsRequest createPutRecordsRequest(List<? extends EventMessage<?>> events, Serializer serializer) {
        return new PutRecordsRequest()
            .withStreamName(eventStorageConfiguration().kinesisStreamName())
            .withRecords(createPutRequestEntries(events, serializer).collect(Collectors.toList()));
    }

    /**
     * Returns a stream of Kinesis "put" request entries that represent the given batch of events. 
     * The given list of {@code events} represents events produced in the context of a single Unit of Work. 
     * Uses the given {@code serializer} to serialize event payload and metadata.
     *
     * @param events     the events to convert to Kinesis "put" request entries.
     * @param serializer the serializer to convert the events' payload and metadata
     * @return stream of Kinesis "put" record entries for the given event batch
     */
    protected abstract Stream<PutRecordsRequestEntry> createPutRequestEntries(List<? extends EventMessage<?>> events, Serializer serializer);

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
