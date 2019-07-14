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

import org.axonframework.common.DateTimeUtils;
import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventUtils;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.EventMessageReader;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.EventMessageWriter;

import static org.axonframework.common.DateTimeUtils.formatInstant;

/**
 * Implementation of a serialized event message that can be used to create a Mongo document.
 */
public class EventEntry implements DomainEventData<Object> {

    private final String aggregateIdentifier;
    private final String aggregateType;
    private final long sequenceNumber;
    private final String timestamp;
    private final Object serializedPayload;
    private final String payloadType;
    private final String payloadRevision;
    private final Object serializedMetaData;
    private final String eventIdentifier;

    /**
     * Constructor used to create a new event entry to store in Mongo.
     *
     * @param event      The actual DomainEvent to store
     * @param serializer Serializer to use for the event to store
     */
    public EventEntry(DomainEventMessage<?> event, Serializer serializer) {
        aggregateIdentifier = event.getAggregateIdentifier();
        aggregateType = event.getType();
        sequenceNumber = event.getSequenceNumber();
        eventIdentifier = event.getIdentifier();
        SerializedObject<?> serializedPayloadObject = event.serializePayload(serializer, byte[].class);
        SerializedObject<?> serializedMetaDataObject = event.serializeMetaData(serializer, byte[].class);
        serializedPayload = serializedPayloadObject.getData();
        payloadType = serializedPayloadObject.getType().getName();
        payloadRevision = serializedPayloadObject.getType().getRevision();
        serializedMetaData = serializedMetaDataObject.getData();
        timestamp = formatInstant(event.getTimestamp());
    }

    /**
     * Creates a new EventEntry based on data provided by Mongo.
     *
     * @param dbObject      Mongo object that contains data to represent an EventEntry
     * @param configuration Configuration containing the property names
     */
    public EventEntry(Item item, DynamoDbEventEntryConfiguration configuration) {
        aggregateIdentifier = item.getString(configuration.aggregateIdentifierProperty());
        aggregateType = item.getString(configuration.typeProperty());
        sequenceNumber = item.getLong(configuration.sequenceNumberProperty());
        serializedPayload = item.get(configuration.payloadProperty());
        timestamp = item.getString(configuration.timestampProperty());
        payloadType = item.getString(configuration.payloadTypeProperty());
        payloadRevision = item.getString(configuration.payloadRevisionProperty());
        serializedMetaData = item.get(configuration.metaDataProperty());
        eventIdentifier = item.getString(configuration.eventIdentifierProperty());
    }

    public EventEntry(byte[] eventData, DynamoDbEventEntryConfiguration configuration, Serializer serializer) {
        this(EventUtils.asDomainEventMessage(toEventMessage(eventData, serializer)), serializer);
    }

    private static EventMessage<Object> toEventMessage(byte[] eventData, Serializer serializer) {
        try {
            EventMessageReader in = new EventMessageReader(new DataInputStream(new ByteArrayInputStream(eventData)),
                                                           serializer);
            return in.readEventMessage();
        } catch (IOException e) {
            // ByteArrayInputStream doesn't throw IOException... anyway...
            throw new RuntimeException("Failed to deserialize an EventMessage", e);
        }
    }
    
    /**
     * Returns the current entry as a DynamoDB {@link Item}.
     *
     * @param configuration The configuration describing binary encoding.
     * @return Document representing the entry
     */
    public Item asItem(DynamoDbEventEntryConfiguration configuration) {
        return new Item()
            .withString(configuration.aggregateIdentifierProperty(), aggregateIdentifier)
            .withString(configuration.typeProperty(), aggregateType)
            .withLong(configuration.sequenceNumberProperty(), sequenceNumber)
            .with(configuration.payloadProperty(), serializedPayload)
            .withString(configuration.timestampProperty(), timestamp)
            .withString(configuration.payloadTypeProperty(), payloadType)
            .withString(configuration.payloadRevisionProperty(), payloadRevision)
            .with(configuration.metaDataProperty(), serializedMetaData)
            .withString(configuration.eventIdentifierProperty(), eventIdentifier);
    }
    
    public byte[] asSerializedBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                EventMessageWriter writer = new EventMessageWriter(dos);
                writer.writeEventData(this);
            }

            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream doesn't throw IOException... anyway...
            throw new RuntimeException("Failed to serialize EventEntry", e);
        }
    }

    @Override
    public String getType() {
        return aggregateType;
    }

    @Override
    public String getAggregateIdentifier() {
        return aggregateIdentifier;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String getEventIdentifier() {
        return eventIdentifier;
    }

    @Override
    public Instant getTimestamp() {
        return DateTimeUtils.parseInstant(timestamp);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SerializedObject<Object> getMetaData() {
        return new SerializedMetaData(serializedMetaData, getRepresentationType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public SerializedObject<Object> getPayload() {
        return new SimpleSerializedObject(serializedPayload, getRepresentationType(), payloadType, payloadRevision);
    }

    private Class<?> getRepresentationType() {
        return (serializedPayload instanceof byte[]) ? byte[].class : String.class;
    }
}
