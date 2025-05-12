/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.command;

/**
 * List of events used in the control protocol between client and the media driver.
 */
public class ControlProtocolEvents
{
    // Clients to Media Driver

    /**
     * Add a Publication.
     */
    public static final int ADD_PUBLICATION = 0x01;

    /**
     * Remove a Publication.
     */
    public static final int REMOVE_PUBLICATION = 0x02;

    /**
     * Add an Exclusive Publication.
     */
    public static final int ADD_EXCLUSIVE_PUBLICATION = 0x03;

    /**
     * Add a Subscriber.
     */
    public static final int ADD_SUBSCRIPTION = 0x04;

    /**
     * Remove a Subscriber.
     */
    public static final int REMOVE_SUBSCRIPTION = 0x05;

    /**
     * Keepalive from Client.
     */
    public static final int CLIENT_KEEPALIVE = 0x06;

    /**
     * Add Destination to an existing Publication.
     */
    public static final int ADD_DESTINATION = 0x07;

    /**
     * Remove Destination from an existing Publication.
     */
    public static final int REMOVE_DESTINATION = 0x08;

    /**
     * Add a Counter to the counters-manager.
     */
    public static final int ADD_COUNTER = 0x09;

    /**
     * Remove a Counter from the counters-manager.
     */
    public static final int REMOVE_COUNTER = 0x0A;

    /**
     * Close indication from Client.
     */
    public static final int CLIENT_CLOSE = 0x0B;

    /**
     * Add Destination for existing Subscription.
     */
    public static final int ADD_RCV_DESTINATION = 0x0C;

    /**
     * Remove Destination for existing Subscription.
     */
    public static final int REMOVE_RCV_DESTINATION = 0x0D;

    /**
     * Request the driver to terminate.
     */
    public static final int TERMINATE_DRIVER = 0x0E;

    /**
     * Add or return a static Counter, i.e. the Counter that cannot be deleted and whose lifecycle is decoupled from
     * the Aeron instance that created it.
     *
     * @since 1.45.0
     */
    public static final int ADD_STATIC_COUNTER = 0x0F;

    /**
     * Invalidate an image.
     */
    public static final int REJECT_IMAGE = 0x10;

    /**
     * Remove a destination by registration id.
     */
    public static final int REMOVE_DESTINATION_BY_ID = 0x11;

    // Media Driver to Clients

    /**
     * Error Response as a result of attempting to process a client command operation.
     */
    public static final int ON_ERROR = 0x0F01;

    /**
     * Subscribed Image buffers are available notification.
     */
    public static final int ON_AVAILABLE_IMAGE = 0x0F02;

    /**
     * New Publication buffers are ready notification.
     */
    public static final int ON_PUBLICATION_READY = 0x0F03;

    /**
     * Operation has succeeded.
     */
    public static final int ON_OPERATION_SUCCESS = 0x0F04;

    /**
     * Inform client of timeout and removal of an inactive Image.
     */
    public static final int ON_UNAVAILABLE_IMAGE = 0x0F05;

    /**
     * New Exclusive Publication buffers are ready notification.
     */
    public static final int ON_EXCLUSIVE_PUBLICATION_READY = 0x0F06;

    /**
     * New Subscription is ready notification.
     */
    public static final int ON_SUBSCRIPTION_READY = 0x0F07;

    /**
     * New counter is ready notification.
     */
    public static final int ON_COUNTER_READY = 0x0F08;

    /**
     * Inform clients of removal of counter.
     */
    public static final int ON_UNAVAILABLE_COUNTER = 0x0F09;

    /**
     * Inform clients of client timeout.
     */
    public static final int ON_CLIENT_TIMEOUT = 0x0F0A;

    /**
     * A response to {@link #ADD_STATIC_COUNTER} command.
     *
     * @since 1.45.0
     */
    public static final int ON_STATIC_COUNTER = 0x0F0B;

    /**
     * Inform clients of error frame received by publication.
     * @since 1.47.0
     */
    public static final int ON_PUBLICATION_ERROR = 0x0F0C;
}
