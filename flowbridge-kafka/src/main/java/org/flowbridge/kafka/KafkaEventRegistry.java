package org.flowbridge.kafka;

import org.flowbridge.core.domain.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * Thread-safe in-memory registry mapping Kafka topics to their Event subscriber callbacks.
 *
 * <p>In the Kafka provider, events arrive from Kafka consumer threads and are dispatched
 * to registered callbacks here. The {@link KafkaSubscriber} registers callbacks; the
 * {@link KafkaPublisher} does NOT use this registry (it goes direct to Kafka).
 * The {@link KafkaConsumerLoop} reads from Kafka and dispatches through this registry.
 */
public class KafkaEventRegistry {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventRegistry.class);

    /** topic → list of Event callbacks */
    private final Map<String, List<java.util.function.Consumer<Event>>> subscribers =
            new ConcurrentHashMap<>();

    /**
     * Register a subscriber for the given topic.
     *
     * @param topic    Kafka topic name
     * @param callback handler that receives the {@link Event}
     */
    public void register(String topic, java.util.function.Consumer<Event> callback) {
        subscribers.computeIfAbsent(topic, t -> new CopyOnWriteArrayList<>()).add(callback);
        log.debug("Registered Kafka subscriber for topic '{}'", topic);
    }

    /**
     * Dispatch event to all registered callbacks for the given topic.
     *
     * @param topic   Kafka topic name
     * @param event   the {@link Event} to dispatch
     */
    public void dispatch(String topic, Event event) {
        List<java.util.function.Consumer<Event>> callbacks = subscribers.get(topic);
        if (callbacks == null || callbacks.isEmpty()) {
            log.debug("No subscribers for Kafka topic '{}'", topic);
            return;
        }
        for (java.util.function.Consumer<Event> cb : callbacks) {
            try {
                cb.accept(event);
            } catch (Exception e) {
                log.error("Kafka subscriber callback failed for topic '{}': {}", topic, e.getMessage(), e);
            }
        }
    }

    /** Returns all currently subscribed topic names. */
    public Set<String> getTopics() {
        return subscribers.keySet();
    }
}
