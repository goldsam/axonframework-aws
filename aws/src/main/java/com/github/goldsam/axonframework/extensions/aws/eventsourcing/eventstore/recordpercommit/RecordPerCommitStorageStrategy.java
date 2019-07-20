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

package com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.recordpercommit;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.AbstractKinesisEventStorageStrategy;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.KinesisEventStorageConfiguration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventUtils;
import org.axonframework.serialization.Serializer;

/**
 * PutRequestPerCommitStorageStrategy
 */
public class RecordPerCommitStorageStrategy extends AbstractKinesisEventStorageStrategy {

    /**
     * Initializes a {@link PutRequestPerCommitStorageStrategy} with default configuration.
     */
    public RecordPerCommitStorageStrategy() {
        super(KinesisEventStorageConfiguration.getDefault());
    }

    /**
     * Initializes a {@link PutRequestPerCommitStorageStrategy} with  given {@code storageConfiguration}.
     *
     * @param storageConfiguration object that configures mapping event data to a Kinesis stream. 
     */
    public RecordPerCommitStorageStrategy(KinesisEventStorageConfiguration storageConfiguration) {
        super(storageConfiguration);
    }

    @Override
    protected void appendEvents(AmazonKinesis kinesisClient, String streamName, List<? extends EventMessage<?>> events, Serializer serializer) {
        kinesisClient.putRecords(new PutRecordsRequest()
            .withStreamName(streamName)
            .withRecords(createPutRequestEntries(events, serializer).collect(Collectors.toList())));
    }
    
    private Stream<PutRecordsRequestEntry> createPutRequestEntries(List<? extends EventMessage<?>> events, Serializer serializer) {
        EventMessage<?> lastEvent = events.get(events.size() - 1);
        return Stream.of(new PutRecordsRequestEntry()
                .withPartitionKey(eventStorageConfiguration()
                    .partitionKeyResolver()
                    .resolvePartitionKey(lastEvent))
                .withData(ByteBuffer.wrap(
                    new CommitEntry(
                        events.stream().map(EventUtils::asDomainEventMessage).collect(Collectors.toList()),
                        serializer)
                    .asSerializedBytes())));
    }
}
