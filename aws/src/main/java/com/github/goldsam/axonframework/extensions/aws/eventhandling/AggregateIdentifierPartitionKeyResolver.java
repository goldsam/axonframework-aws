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

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;

/**
 * {@link PartitionKeyResolver} implementation that returns the identifier of the aggregate 
 * that generated the event if the {@code eventMessage} implements the {@link DomainEventMessage} 
 * interface; otherwise, the package name of the Message's payload is returned. This intent of 
 * this strategy is for all events for a specific aggregate root to be processed through a 
 * common Kinesis Stream Shard which provides a total ordering over events for any single aggregate.
 * 
 * @author Sam Goldmann
 * @since 1.0
 */
public class AggregateIdentifierPartitionKeyResolver implements PartitionKeyResolver {

    @Override
    public String resolvePartitionKey(EventMessage<?> eventMessage) {
        return (eventMessage instanceof DomainEventMessage<?>) 
            ? ((DomainEventMessage<?>)eventMessage).getAggregateIdentifier() 
            : eventMessage.getPayloadType().getPackage().getName();
    }
}


