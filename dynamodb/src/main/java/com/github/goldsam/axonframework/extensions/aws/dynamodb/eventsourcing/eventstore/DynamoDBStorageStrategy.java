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
import java.util.List;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.Serializer;

/**
 * Interface for a strategy that persists events to a DynamoDB table.
 * Events are provided in "commits", which represent a number of events generated 
 * by the same aggregate, inside a single Unit of Work. 
 * Implementations may choose to use this fact, or ignore it.
 */
public interface DynamoDBStorageStrategy {
    /**
     * Persist the given list of {@code events} using the given {@code AmazonDynamoDB} client. 
     * Uses the given {@code serializer} to serialize the payload and metadata of the events.
     *
     * @param dynamoDB    Amazon DynamoDB client.
     * @param tableName   Name of table to persist serialized events to.
     * @param events      the event messages to append to the "put" request.
     * @param serializer  the serializer used to serialize the event payload and metadata
     */
    void appendEvents(AmazonDynamoDB dynamoDB, String tableName,  List<? extends EventMessage<?>> events, Serializer serializer);    
}
