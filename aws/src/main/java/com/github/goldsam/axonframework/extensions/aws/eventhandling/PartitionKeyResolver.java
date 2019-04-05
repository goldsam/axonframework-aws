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

import org.axonframework.eventhandling.EventMessage;

/**
 * Provides the Kinesis Stream Partition Key used when sending a given EventMessage. 
 * The partition key determines which Kinesis Stream Shard will receive the event message. 
 */
@FunctionalInterface
public interface PartitionKeyResolver {

    /**
     * Returns the Partition Key to use when sending the given {@code eventMessage} to the Kinesis Stream.
     *
     * @param eventMessage The EventMessage to resolve the partition key for
     * @return the partition key for the event message
     */
    String resolvePartitionKey(EventMessage<?> eventMessage);

}
