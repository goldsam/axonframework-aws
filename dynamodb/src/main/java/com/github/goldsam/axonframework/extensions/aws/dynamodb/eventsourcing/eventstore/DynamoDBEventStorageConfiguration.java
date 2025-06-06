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

import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventhandling.AggregateIdentifierPartitionKeyResolver;
import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventhandling.PackageStreamNameResolver;
import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventhandling.PartitionKeyResolver;
import com.github.goldsam.axonframework.extensions.aws.dynamodb.eventhandling.StreamNameResolver;

/**
 * Amazon Kinesis event storage configuration. 
 */
public class DynamoDBEventStorageConfiguration {

    private final String eventTableName;
    
    /**
     * Returns the default {@link DynamoDbEventEntryConfiguration}.
     *
     * @return the default {@link DynamoDbEventEntryConfiguration}
     */
    public static DynamoDBEventStorageConfiguration getDefault() {
        return builder().build();
    }

    private DynamoDBEventStorageConfiguration(Builder builder) {
        eventTableName = builder.eventTableName;
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
     * Gets the name of the DynamoDB table used to store domain events.
     * 
     * @return the name of the DynamoDB table used to store domain events
     */
    public String eventTableName() {
        return eventTableName;
    }

    public static class Builder {
        
        private String eventTableName;
        
        public Builder eventTableName(String eventTableName) {
            this.eventTableName = eventTableName;
            return this;
        }
        
        public DynamoDBEventStorageConfiguration build() {
            return new DynamoDBEventStorageConfiguration(this);
        }
    }
}
