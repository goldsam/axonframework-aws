/*
 * Copyright (c) 2010-2014. Axon Framework
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

package com.github.goldsam.axonframework.extensions.aws.dynamodb.eventhandling;

import org.axonframework.eventhandling.DomainEventData;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventData;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;

import java.io.DataOutput;
import java.io.IOException;

import static org.axonframework.common.DateTimeUtils.formatInstant;

/**
 * Writer that writes Event Messages onto a an OutputStream. The format of the message makes them suitable to be read
 * back in using a {@link EventMessageReader}. This writer distinguishes between DomainEventMessage and plain
 * EventMessage when writing. The reader will reconstruct an aggregate implementation for the same message type (i.e.
 * DomainEventMessage or EventMessage).
 *
 * @author Allard Buijze
 * @since 1.0
 */
public class EventMessageWriter {

    private final DataOutput out;

    /**
     * Creates a new EventMessageWriter writing data to the specified underlying {@code output}.
     *
     * @param output     the underlying output
     */
    public EventMessageWriter(DataOutput output) {
        this.out = output;
    }

    /**
     * Writes the given {@code eventMessage} to the underling output.
     *
     * @param eventMessage the EventMessage to write to the underlying output
     * @param serializer The serializer to deserialize payload and metadata with
     * @throws IOException when any exception occurs writing to the underlying stream
     */
    public void writeEventMessage(EventMessage<?> eventMessage, Serializer serializer) throws IOException {
        if (DomainEventMessage.class.isInstance(eventMessage)) {
            out.writeByte(EventMessageType.DOMAIN_EVENT_MESSAGE.getTypeByte());
        } else {
            out.writeByte(EventMessageType.EVENT_MESSAGE.getTypeByte());
        }
        out.writeUTF(eventMessage.getIdentifier());
        out.writeUTF(formatInstant(eventMessage.getTimestamp()));
        if (eventMessage instanceof DomainEventMessage) {
            DomainEventMessage<?> domainEventMessage = (DomainEventMessage<?>) eventMessage;
            out.writeUTF(domainEventMessage.getAggregateIdentifier());
            out.writeLong(domainEventMessage.getSequenceNumber());
        }
        SerializedObject<byte[]> serializedPayload = eventMessage.serializePayload(serializer, byte[].class);
        SerializedObject<byte[]> serializedMetaData = eventMessage.serializeMetaData(serializer, byte[].class);

        out.writeUTF(serializedPayload.getType().getName());
        String revision = serializedPayload.getType().getRevision();
        out.writeUTF(revision == null ? "" : revision);
        out.writeInt(serializedPayload.getData().length);
        out.write(serializedPayload.getData());
        out.writeInt(serializedMetaData.getData().length);
        out.write(serializedMetaData.getData());
    }

    /**
     * Writes the given {@code eventData} to the underlying output.
     *
     * @param eventData the EventMessage to write to the underlying output
     * @param serializer The serializer to deserialize payload and metadata with
     * @throws IOException when any exception occurs writing to the underlying stream
     */
    public void writeEventData(EventData<?> eventData) throws IOException {
        if (DomainEventData.class.isInstance(eventData)) {
            out.writeByte(EventMessageType.DOMAIN_EVENT_MESSAGE.getTypeByte());
        } else {
            out.writeByte(EventMessageType.EVENT_MESSAGE.getTypeByte());
        }
        out.writeUTF(eventData.getEventIdentifier());
        out.writeUTF(formatInstant(eventData.getTimestamp()));
        if (eventData instanceof DomainEventData) {
            DomainEventData<?> domainEventData = (DomainEventData<?>)eventData;
            out.writeUTF(domainEventData.getAggregateIdentifier());
            out.writeLong(domainEventData.getSequenceNumber());
        }
        SerializedObject<?> serializedPayload = eventData.getPayload();
        SerializedObject<?> serializedMetaData = eventData.getMetaData();

        out.writeUTF(serializedPayload.getType().getName());
        String revision = serializedPayload.getType().getRevision();
        out.writeUTF(revision == null ? "" : revision);
        writeSerializedObject(serializedPayload);
        writeSerializedObject(serializedMetaData);
    }

    private void writeSerializedObject(SerializedObject<?> serializedObject) throws IOException {
        byte[] data = (byte[])serializedObject.getData();
        out.writeInt(data.length);
        out.write(data);
    }
}
