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

package com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore;

import com.github.goldsam.axonframework.extensions.aws.eventhandling.AggregateIdentifierPartitionKeyResolver;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.PartitionKeyResolver;

/**
 * Amazon Kinesis event storage configuration. 
 */
public class KinesisEventStorageConfiguration {

    private final PartitionKeyResolver partitionKeyResolver;

    private final String kinesisStreamName;

    /**
     * Returns the default {@link DynamoDbEventEntryConfiguration}.
     *
     * @return the default {@link DynamoDbEventEntryConfiguration}
     */
    public static KinesisEventStorageConfiguration getDefault() {
        return builder().build();
    }

    private KinesisEventStorageConfiguration(Builder builder) {
        partitionKeyResolver = builder.partitionKeyResolver;
        kinesisStreamName = builder.kinesisStreamName;
    }

    /**
     * Returns a new Builder for an {@link DynamoDbEventEntryConfiguration} initialized with default settings.
     *
     * @return a new Builder with default settings
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the strategy for resolving a Kinesis stream partition key.
     * 
     * @return the strategy for resolving a Kinesis stream partition key.
     */
    public PartitionKeyResolver partitionKeyResolver() {
        return partitionKeyResolver;
    }

    /**
     * Gets the name of the target Kinesis stream.
     * 
     * @return the name of the target Kinesis stream.
     */
    public String kinesisStreamName() {
        return kinesisStreamName;
    }

    public static class Builder {

        private PartitionKeyResolver partitionKeyResolver = new AggregateIdentifierPartitionKeyResolver();

        private String kinesisStreamName = "events";

        public Builder partitionKeyResolver(PartitionKeyResolver partitionKeyResolver) {
            this.partitionKeyResolver = partitionKeyResolver;
            return this;
        }

        public Builder kinesisStreamName(String kinesisStreamName) {
            this.kinesisStreamName = kinesisStreamName;
            return this;
        }
        
        public KinesisEventStorageConfiguration build() {
            return new KinesisEventStorageConfiguration(this);
        }
    }
}
