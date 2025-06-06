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


package com.github.goldsam.axonframework.extensions.aws.dynamodb.eventsourcing.eventstore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.jdbc.PersistenceExceptionResolver;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.TrackedEventData;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.eventsourcing.eventstore.BatchingEventStorageEngine;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventsourcing.eventstore.recordpercommit.RecordPerCommitStorageStrategy;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * {@link EventStorageEngine} implementation that writes events to an Amazon Kinesis Stream.
 * The expectation is that this Kinesis Stream is used to Stage events before they
 * are written to DynamoDB, typically using a Lambda function.
 */
public class DynamoDBEventStorageEngine extends BatchingEventStorageEngine {

    private final AmazonDynamoDB amazonDynamoDB;
    
    private final DynamoDBEventStorageConfiguration eventStorageConfiguration;
    
    private final DynamoDBStorageStrategy storageStrategy;

    /**
     * Instantiate a {@link MongoEventStorageEngine} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoTemplate} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if any of them is {@code null}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link MongoEventStorageEngine} instance
     */
    protected DynamoDBEventStorageEngine(Builder builder) {
        super(builder);
        this.amazonDynamoDB  = builder.amazonDynamoDB;
        this.storageStrategy = builder.storageStrategy;
        ensureIndexes();
    }

    /**
     * Instantiate a Builder to be able to create a {@link DynamoDBEventStorageEngine}.
     * <p>
     * The following configurable fields have defaults:
     * <ul>
     * <li>The snapshot {@link Serializer} defaults to {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@link EventUpcaster} defaults to an {@link org.axonframework.serialization.upcasting.event.NoOpEventUpcaster}.</li>
     * <li>The event Serializer defaults to a {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@code snapshotFilter} defaults to a {@link Predicate} which returns {@code true} regardless.</li>
     * <li>The {@code batchSize} defaults to an integer of size {@code 100}.</li>
     * <li>The {@link DynamoDBStorageStrategy} defaults to a {@link RecordPerCommitStorageStrategy}.</li>
     * </ul>
     * <p>
     *
     * @return a Builder to be able to create a {@link MongoEventStorageEngine}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Make sure an index is created on the collection that stores domain events.
     */
    private void ensureIndexes() {
        // storageStrategy.ensureIndexes(template.eventCollection(), template.snapshotCollection());
    }
 
    @Override
    protected void appendEvents(List<? extends EventMessage<?>> events, Serializer serializer) {
        if (!events.isEmpty()) {
            try {
                storageStrategy.appendEvents(kinesisClient, events, serializer);
            } catch (Exception e) {
                handlePersistenceException(e, events.get(0));
            }
        }
    }

    @Override
    protected void storeSnapshot(DomainEventMessage<?> snapshot, Serializer serializer) {
        throw new UnsupportedOperationException();
        // try {
        //     storageStrategy.appendSnapshot(template.snapshotCollection(), snapshot, serializer);
        //     storageStrategy.deleteSnapshots(
        //             template.snapshotCollection(), snapshot.getAggregateIdentifier(), snapshot.getSequenceNumber()
        //     );
        // } catch (Exception e) {
        //     handlePersistenceException(e, snapshot);
        // }
    }

    @Override
    protected Stream<? extends DomainEventData<?>> readSnapshotData(String aggregateIdentifier) {
        throw new UnsupportedOperationException();
        // return storageStrategy.findSnapshots(template.snapshotCollection(), aggregateIdentifier);
    }

    @Override
    protected List<? extends DomainEventData<?>> fetchDomainEvents(String aggregateIdentifier, long firstSequenceNumber, int batchSize) {
        // return storageStrategy
        //     .findDomainEvents(template.eventCollection(), aggregateIdentifier, firstSequenceNumber, batchSize);
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<? extends TrackedEventData<?>> fetchTrackedEvents(TrackingToken lastToken, int batchSize) {
        // return storageStrategy.findTrackedEvents(template.eventCollection(), lastToken, batchSize);
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Long> lastSequenceNumberFor(String aggregateIdentifier) {
        // return storageStrategy.lastSequenceNumberFor(template.eventCollection(), aggregateIdentifier);
        throw new UnsupportedOperationException();
    }

    @Override
    public TrackingToken createTailToken() {
        // return storageStrategy.createTailToken(template.eventCollection());
        throw new UnsupportedOperationException();
    }

    @Override
    public TrackingToken createHeadToken() {
        return createTokenAt(Instant.now());
    }

    @Override
    public TrackingToken createTokenAt(Instant dateTime) {
        // return MongoTrackingToken.of(dateTime, Collections.emptyMap());
        throw new UnsupportedOperationException();
    }

    /**
     * Builder class to instantiate a {@link DynamoDBEventStorageEngine}.
     * <p>
     * The following configurable fields have defaults:
     * <ul>
     * <li>The snapshot {@link Serializer} defaults to {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@link EventUpcaster} defaults to an {@link org.axonframework.serialization.upcasting.event.NoOpEventUpcaster}.</li>
     * <li>The event Serializer defaults to a {@link org.axonframework.serialization.xml.XStreamSerializer}.</li>
     * <li>The {@code snapshotFilter} defaults to a {@link Predicate} which returns {@code true} regardless.</li>
     * <li>The {@code batchSize} defaults to an integer of size {@code 100}.</li>
     * <li>The {@link DynamoDBStorageStrategy} defaults to a {@link RecordPerCommitStorageStrategy}.</li>
     * </ul>
     * <p>
     */
    public static class Builder extends BatchingEventStorageEngine.Builder {

        private AmazonKinesis kinesisClient;
         
        private DynamoDBStorageStrategy storageStrategy = new RecordPerCommitStorageStrategy();

        private Builder() {
        }

        @Override
        public Builder snapshotSerializer(Serializer snapshotSerializer) {
            super.snapshotSerializer(snapshotSerializer);
            return this;
        }

        @Override
        public Builder upcasterChain(EventUpcaster upcasterChain) {
            super.upcasterChain(upcasterChain);
            return this;
        }

        @Override
        public Builder persistenceExceptionResolver(PersistenceExceptionResolver persistenceExceptionResolver) {
            super.persistenceExceptionResolver(persistenceExceptionResolver);
            return this;
        }

        @Override
        public Builder eventSerializer(Serializer eventSerializer) {
            super.eventSerializer(eventSerializer);
            return this;
        }

        @Override
        public Builder snapshotFilter(Predicate<? super DomainEventData<?>> snapshotFilter) {
            super.snapshotFilter(snapshotFilter);
            return this;
        }

        @Override
        public Builder batchSize(int batchSize) {
            super.batchSize(batchSize);
            return this;
        }

        public Builder kinesisClient(AmazonKinesis kinesisClient) {
            assertNonNull(kinesisClient, "kinesisClient may not be null");
            this.kinesisClient = kinesisClient;
            return this;
        }

        /**
         * Sets the {@link DynamoDBStorageStrategy} specifying how to store and retrieve events and snapshots from the
         * collections. Defaults to a {@link DocumentPerEventStorageStrategy}, causing every event and snapshot to be
         * stored in a separate Mongo Document.
         *
         * @param storageStrategy the {@link DynamoDBStorageStrategy} specifying how to store and retrieve events and snapshots
         *                        from the collections
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder storageStrategy(DynamoDBStorageStrategy storageStrategy) {
            assertNonNull(storageStrategy, "storageStrategy may not be null");
            this.storageStrategy = storageStrategy;
            return this;
        }

        /**
         * Initializes a {@link MongoEventStorageEngine} as specified through this Builder.
         *
         * @return a {@link MongoEventStorageEngine} as specified through this Builder
         */
        public DynamoDBEventStorageEngine build() {
            return new DynamoDBEventStorageEngine(this);
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        @Override
        protected void validate() throws AxonConfigurationException {
            super.validate();
            assertNonNull(kinesisClient, "The kinesisClient is a hard requirement and should be provided");
        }
    }
}
