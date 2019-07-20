/*
 * Copyright (c) 2019. Samuel Goldmann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.recordperevent;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.PartitionKeyResolver;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.AbstractKinesisEventStorageStrategy;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.KinesisEventStorageConfiguration;
import java.util.Map;
import java.util.stream.IntStream;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventUtils;
import org.axonframework.serialization.Serializer;

/**
 * Storage strategy that creates a distinct Kinesis record for each {@link EventMessage}.
 * Events are published using a strategy that uses a bulk put operation for all records
 * targeting a distinct stream partitions (see 
 * <a href="https://brandur.org/kinesis-order">Guaranteeing Order with Kinesis Bulk Puts</a>).
 */
public class RecordPerEventStorageStrategy extends AbstractKinesisEventStorageStrategy {

    /**
     * Initializes a {@link PutRequestPerCommitStorageStrategy} with default configuration.
     */
    public RecordPerEventStorageStrategy() {
        super(KinesisEventStorageConfiguration.getDefault());
    }

    /**
     * Initializes a {@link PutRequestPerCommitStorageStrategy} with  given {@code storageConfiguration}.
     *
     * @param storageConfiguration object that configures mapping event data to a Kinesis stream. 
     */
    public RecordPerEventStorageStrategy(KinesisEventStorageConfiguration storageConfiguration) {
        super(storageConfiguration);
    }
    
    @Override
    protected void appendEvents(AmazonKinesis kinesisClient, String streamName, List<? extends EventMessage<?>> events, Serializer serializer) {
        PartitionKeyResolver partitionKeyResolver = eventStorageConfiguration().partitionKeyResolver();
        Map<String, List<EventMessage<?>>> eventGroups = events.stream()
                .collect(Collectors.groupingBy(event -> partitionKeyResolver.resolvePartitionKey(event)));
        
        appendSequentialEventGroups(
                kinesisClient,
                streamName,
                eventGroups.entrySet().stream()
                    .filter(eventPartitionKeyGroup -> eventPartitionKeyGroup.getValue().size() == 1),
                serializer);        
        
        appendNonSequentialEventGroups(
                kinesisClient,
                streamName,
                eventGroups.entrySet().stream()
                    .filter(eventPartitionKeyGroup -> eventPartitionKeyGroup.getValue().size() > 1),
                serializer);
    }
    
    private void appendSequentialEventGroups(AmazonKinesis kinesisClient, String streamName, Stream<Map.Entry<String, List<EventMessage<?>>>> sequentialEventGroups, Serializer serializer) {
        sequentialEventGroups
                .forEach(partitionKeyAndEvents -> appendSequentialEvents(
                        kinesisClient,
                        streamName,
                        partitionKeyAndEvents.getKey(),
                        partitionKeyAndEvents.getValue(),
                        serializer));
    }
    
    private void appendSequentialEvents(AmazonKinesis kinesisClient, String streamName, String partitionKey, List<EventMessage<?>> events, Serializer serializer) {
        events.stream().reduce(
                (String)null, 
                (sequentNumberForOrdering, event) -> appendSequentialEvent(kinesisClient, streamName, partitionKey, event, serializer, sequentNumberForOrdering),
                (prevSequentNumberForOrdering, nextSequentNumberForOrdering) -> nextSequentNumberForOrdering);
    }
    
    private String appendSequentialEvent(AmazonKinesis kinesisClient, String streamName, String partitionKey, EventMessage<?> event, Serializer serializer, String sequenceNumberForOrdering) {
        PutRecordRequest putRecordRequest = new PutRecordRequest()
                .withStreamName(streamName)
                .withPartitionKey(partitionKey)
                .withSequenceNumberForOrdering(sequenceNumberForOrdering)
                .withData(serializeEvent(event, serializer));
        return kinesisClient.putRecord(putRecordRequest).getSequenceNumber();
    }
    
    private void appendNonSequentialEventGroups(AmazonKinesis kinesisClient, String streamName, Stream<Map.Entry<String, List<EventMessage<?>>>> nonSequentialEventGroups, Serializer serializer) {
        List<PutRecordsRequestEntry> putRecordsRequestEntries = nonSequentialEventGroups
                .map(partitionKeyAndEvent -> new PutRecordsRequestEntry()
                    .withPartitionKey(partitionKeyAndEvent.getKey())
                    .withData(serializeEvent(partitionKeyAndEvent.getValue().get(0), serializer)))
                .collect(Collectors.toList());
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest()
                .withStreamName(streamName)
                .withRecords(putRecordsRequestEntries);
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        while (putRecordsResult.getFailedRecordCount() > 0) {
            putRecordsRequestEntries = collectFailedPutRecordsRequestEntries(
                    putRecordsRequestEntries, putRecordsResult.getRecords());
            putRecordsRequest.setRecords(putRecordsRequestEntries);
            putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        }
    }
    
    private List<PutRecordsRequestEntry> collectFailedPutRecordsRequestEntries(List<PutRecordsRequestEntry> requestEntries, List<PutRecordsResultEntry> resultEntries) {
        return IntStream.range(0, resultEntries.size())
                .filter(i -> resultEntries.get(i).getErrorCode() != null)
                .mapToObj(i -> requestEntries.get(i))
                .collect(Collectors.toList());
    }
    
    private ByteBuffer serializeEvent(EventMessage<?> event, Serializer serializer) {
        return ByteBuffer.wrap(new EventEntry(EventUtils.asDomainEventMessage(event), serializer).asSerializedBytes());
    }
}
