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

package com.github.goldsam.axonframework.extensions.aws.eventhandling;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.Record;
import java.util.Optional;
import org.axonframework.eventhandling.EventMessage;

/**
 * Interface for converting Kinsis Stream Records to and from Axon Messages.
 */
public interface KinesisMessageConverter {

    /**
     * Creates an {@link com.amazonaws.services.kinesis.model.PutRecordsRequestEntry} from a given {@code eventMessage}.
     * 
     * @param eventMessage The event message data to create the Kenisis {@link PutRecordsRequestEntry} from.
     * @return an {@link com.amazonaws.services.kinesis.model.PutRecordsRequestEntry} which can be 
     *         published to a Kinesis Stream by a "put" operation.
     */
    PutRecordsRequestEntry createPutRecordsRequestEntry(EventMessage<?> eventMessage);
   
    /**
     * Reconstructs an {@link EventMessage} from a given {@code record}.
     * 
     * @param record a {@link com.amazonaws.services.kinesis.model.Record} read from an Kenesis Stream by a "get" operation.
     * @return The event message to publish on the local event procoessors.res
     */
    Optional<EventMessage<?>> readRecord(Record record);
}
 