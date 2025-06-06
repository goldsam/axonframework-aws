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

package com.github.goldsam.axonframework.extensions.aws.kinesis.eventhandling;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;

/**
 * {@link PartitionKeyResolver} implementation that returns the identifier of the 
 * aggregate that generated the event if the {@code eventMessage} implements the 
 * {@link DomainEventMessage} interface; otherwise, the package name of the Message 
 * payload is returned. The intent of this strategy is for all events for a specific 
 * aggregate root to be processed through a common Kinesis Stream Shard. This ensures
 * events are processed in their correct order.
 */
public class AggregateIdentifierPartitionKeyResolver implements PartitionKeyResolver {

    @Override
    public String resolvePartitionKey(EventMessage<?> eventMessage) {
        return (eventMessage instanceof DomainEventMessage<?>) 
            ? ((DomainEventMessage<?>)eventMessage).getAggregateIdentifier() 
            : eventMessage.getPayloadType().getPackage().getName();
    }
}


