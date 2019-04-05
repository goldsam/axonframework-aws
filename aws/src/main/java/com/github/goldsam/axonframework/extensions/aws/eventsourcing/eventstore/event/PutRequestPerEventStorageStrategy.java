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

package com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.event;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.AbstractKinesisEventStorageStrategy;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.commit.CommitEntry;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.KinesisEventStorageConfiguration;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventUtils;
import org.axonframework.serialization.Serializer;

/**
 * PutRequestPerCommitStorageStrategy
 */
public class PutRequestPerEventStorageStrategy extends AbstractKinesisEventStorageStrategy {

    /**
     * Initializes a {@link PutRequestPerCommitStorageStrategy} with default configuration.
     */
    public PutRequestPerEventStorageStrategy() {
        super(KinesisEventStorageConfiguration.getDefault());
    }

    /**
     * Initializes a {@link PutRequestPerCommitStorageStrategy} with  given {@code storageConfiguration}.
     *
     * @param storageConfiguration object that configures mapping event data to a Kinesis stream. 
     */
    public PutRequestPerEventStorageStrategy(KinesisEventStorageConfiguration storageConfiguration) {
        super(storageConfiguration);
    }

    @Override
    protected Stream<PutRecordsRequestEntry> createPutRequestEntries(List<? extends EventMessage<?>> events, Serializer serializer) {
        return events.stream()
            .map(EventUtils::asDomainEventMessage)
            .map(event -> {
                return new PutRecordsRequestEntry()
                    .withPartitionKey(eventStorageConfiguration()
                        .partitionKeyResolver()
                        .resolvePartitionKey(event))
                    .withData(ByteBuffer.wrap(new EventEntry(event, serializer).asSerializedBytes()));
            });
    }
}
