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

package com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.commit;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.github.goldsam.axonframework.extensions.aws.eventhandling.EventMessageWriter;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.event.DynamoDbEventEntryConfiguration;
import com.github.goldsam.axonframework.extensions.aws.eventsourcing.eventstore.event.EventEntry;

import static org.axonframework.common.DateTimeUtils.formatInstant;

/**
 */
public class CommitEntry {

    private final String aggregateIdentifier;
    private final String aggregateType;
    private final long firstSequenceNumber;
    private final long lastSequenceNumber;
    private final String firstTimestamp;
    private final String lastTimestamp;
    private final EventEntry[] eventEntries;
    private final String lastEventIdentifier;

    /**
     * Constructor used to create a new event entry to store in Mongo.
     *
     * @param serializer Serializer to use for the event to store
     * @param events     The events contained in this commit
     */
    public CommitEntry(List<? extends DomainEventMessage<?>> events, Serializer serializer) {
        DomainEventMessage<?> firstEvent = events.get(0);
        DomainEventMessage<?> lastEvent = events.get(events.size() - 1);
        firstSequenceNumber = firstEvent.getSequenceNumber();
        firstTimestamp = formatInstant(firstEvent.getTimestamp());
        lastTimestamp = formatInstant(lastEvent.getTimestamp());
        lastSequenceNumber = lastEvent.getSequenceNumber();
        aggregateIdentifier = lastEvent.getAggregateIdentifier();
        lastEventIdentifier = lastEvent.getIdentifier();
        aggregateType = lastEvent.getType();
        eventEntries = new EventEntry[events.size()];
        for (int i = 0, eventsLength = events.size(); i < eventsLength; i++) {
            DomainEventMessage<?> event = events.get(i);
            eventEntries[i] = new EventEntry(event, serializer);
        }
    }

    /**
     * Creates a new CommitEntry based on data provided by Mongo.
     *
     * @param item            Mongo object that contains data to represent a CommitEntry
     * @param commitConfiguration commit entry specific configuration
     * @param eventConfiguration  event entry specific configuration
     */
    @SuppressWarnings("unchecked")
    public CommitEntry(Item item, DynamoDbCommitEntryConfiguration commitConfiguration, DynamoDbEventEntryConfiguration eventConfiguration, Serializer serializer) {
        this.aggregateIdentifier = item.getString(eventConfiguration.aggregateIdentifierProperty());
        this.firstSequenceNumber = item.getLong(commitConfiguration.firstSequenceNumberProperty());
        this.lastSequenceNumber = item.getLong(commitConfiguration.lastSequenceNumberProperty());
        this.firstTimestamp = item.getString(commitConfiguration.firstTimestampProperty());
        this.lastTimestamp = item.getString(commitConfiguration.lastTimestampProperty());
        this.aggregateType = item.getString(eventConfiguration.typeProperty());
        this.lastEventIdentifier = item.getString(eventConfiguration.eventIdentifierProperty());
        List<byte[]> entries = item.getList(commitConfiguration.eventsProperty());
        eventEntries = new EventEntry[entries.size()];
        for (int i = 0, entriesSize = entries.size(); i < entriesSize; i++) {
            eventEntries[i] = new EventEntry(entries.get(i), eventConfiguration, serializer);
        }
    }

    /**
     * Returns the event entries stored as part of this commit.
     *
     * @return The event instances stored in this entry
     */
    public EventEntry[] getEvents() {
        return eventEntries;
    }

    /**
     * Returns the current CommitEntry as a DynamoDB {@link Item}.
     *
     * @param commitConfiguration Configuration of commit-specific properties on the document
     * @param eventConfiguration  Configuration of event-related properties on the document
     * @return DynamoDB Item representing this CommitEntry.
     */
    public Item asItem(DynamoDbCommitEntryConfiguration commitConfiguration, DynamoDbEventEntryConfiguration eventConfiguration) {
        List<byte[]> events = new ArrayList<>();
        for (EventEntry eventEntry : eventEntries) {
            events.add(eventEntry.asSerializedBytes());
        }

        return new Item()
            .withString(eventConfiguration.aggregateIdentifierProperty(), aggregateIdentifier)
            .withLong(eventConfiguration.sequenceNumberProperty(), lastSequenceNumber)
            .withString(eventConfiguration.eventIdentifierProperty(), lastEventIdentifier)
            .withLong(commitConfiguration.lastSequenceNumberProperty(), lastSequenceNumber)
            .withLong(commitConfiguration.firstSequenceNumberProperty(), firstSequenceNumber)
            .withString(eventConfiguration.timestampProperty(), firstTimestamp)
            .withString(commitConfiguration.firstTimestampProperty(), firstTimestamp)
            .withString(commitConfiguration.lastTimestampProperty(), lastTimestamp)
            .withString(eventConfiguration.typeProperty(), aggregateType)
            .withList(commitConfiguration.eventsProperty(), events);
    }

    public byte[] asSerializedBytes() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeUTF(aggregateIdentifier);
                dos.writeUTF(aggregateType);

                int entriesLength = eventEntries.length; 
                dos.writeInt(entriesLength);
                EventMessageWriter writer = new EventMessageWriter(dos);
                for (int i = 0; i < entriesLength; i++) {
                    writer.writeEventData(eventEntries[i]);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream doesn't throw IOException... anyway...
            throw new RuntimeException("Failed to serialize an EventMessage", e);
        }
    }
}
