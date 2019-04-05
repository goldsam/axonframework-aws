package com.github.goldsam.axonframework.extensions.aws.eventhandling.guice;

/**
 * Service
 */
public interface Service {

    void start() throws Exception;

    void stop();
}
