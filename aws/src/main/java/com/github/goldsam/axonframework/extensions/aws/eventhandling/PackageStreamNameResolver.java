package com.github.goldsam.axonframework.extensions.aws.eventhandling;

import org.axonframework.eventhandling.EventMessage;

/**
 * {@link StreamNameResolver} implementation that uses the package name 
 * of the Message's payload as the stream name.
 */
public class PackageStreamNameResolver implements StreamNameResolver {

    @Override
    public String resolveStreamName(EventMessage<?> event) {
        return event.getPayloadType().getPackage().getName();
    }
}
