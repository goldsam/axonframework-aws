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
import com.github.goldsam.axonframework.extensions.aws.eventhandling.PackageStreamNameResolver;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.PartitionKeyResolver;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.StreamNameResolver;

/**
 * Amazon Kinesis event storage configuration. 
 */
public class KinesisEventStorageConfiguration {

    private final StreamNameResolver streamNameResolver;
    
    private final PartitionKeyResolver partitionKeyResolver;

    /**
     * Returns the default {@link DynamoDbEventEntryConfiguration}.
     *
     * @return the default {@link DynamoDbEventEntryConfiguration}
     */
    public static KinesisEventStorageConfiguration getDefault() {
        return builder().build();
    }

    private KinesisEventStorageConfiguration(Builder builder) {
        streamNameResolver = builder.streamNameResolver;
        partitionKeyResolver = builder.partitionKeyResolver;
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
     * Gets the strategy for resolving the name of the target Kinesis Stream.
     * 
     * @return the strategy for resolving the name of the target Kinesis Stream.
     */
    public StreamNameResolver streamNameResolver() {
        return streamNameResolver;
    }
    
    /**
     * Gets the strategy for resolving a Kinesis stream partition key.
     * 
     * @return the strategy for resolving a Kinesis stream partition key.
     */
    public PartitionKeyResolver partitionKeyResolver() {
        return partitionKeyResolver;
    }

    public static class Builder {
        
        private StreamNameResolver streamNameResolver = new PackageStreamNameResolver();

        private PartitionKeyResolver partitionKeyResolver = new AggregateIdentifierPartitionKeyResolver();

        public Builder partitionKeyResolver(StreamNameResolver streamNameResolver) {
            this.streamNameResolver = streamNameResolver;
            return this;
        }
        
        public Builder partitionKeyResolver(PartitionKeyResolver partitionKeyResolver) {
            this.partitionKeyResolver = partitionKeyResolver;
            return this;
        }
        
        public KinesisEventStorageConfiguration build() {
            return new KinesisEventStorageConfiguration(this);
        }
    }
}
