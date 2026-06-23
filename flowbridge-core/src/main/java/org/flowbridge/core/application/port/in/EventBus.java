package org.flowbridge.core.application.port.in;

import org.flowbridge.core.domain.model.ReplayOptions;
import java.util.Map;
import java.util.function.Consumer;

public interface EventBus {
    
    /**
     * Publishes an event to the specified topic.
     *
     * @param topic the target topic name
     * @param event the event payload object to publish
     */
    void publish(String topic, Object event);

    /**
     * Publishes an event with custom headers to the specified topic.
     *
     * @param topic   the target topic name
     * @param event   the event payload object to publish
     * @param headers additional metadata headers
     */
    void publish(String topic, Object event, Map<String, String> headers);

    /**
     * Subscribes a consumer callback to the specified topic.
     *
     * @param topic     the topic name to subscribe to
     * @param eventType the expected class type of the event payload
     * @param consumer  the callback handler invoked on new events
     * @param <T>       the event type parameter
     */
    <T> void subscribe(String topic, Class<T> eventType, Consumer<T> consumer);

    /**
     * Triggers a replay of events on the specified topic based on the provided options.
     *
     * @param topic   the topic name to replay events from
     * @param options configurations specifying the start position (all, timestamp, or offset)
     */
    void replay(String topic, ReplayOptions options);
}
