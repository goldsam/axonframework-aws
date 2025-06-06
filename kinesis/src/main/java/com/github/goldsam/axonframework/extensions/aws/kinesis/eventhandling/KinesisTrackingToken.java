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

import java.io.Serializable;
import org.axonframework.eventhandling.TrackingToken;

/**
 * Use to track messages sent and received using Kinesis streams.
 */
public class KinesisTrackingToken implements TrackingToken, Serializable {
    
    private String sequenceNumber;
    
    public KinesisTrackingToken(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public TrackingToken lowerBound(TrackingToken other) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public TrackingToken upperBound(TrackingToken other) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean covers(TrackingToken other) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
