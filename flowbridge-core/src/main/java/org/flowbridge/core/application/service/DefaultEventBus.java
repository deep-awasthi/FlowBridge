package org.flowbridge.core.application.service;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.port.out.Publisher;
import org.flowbridge.core.application.port.out.Subscriber;
import org.flowbridge.core.application.port.out.ReplayableStore;
import org.flowbridge.core.application.port.out.DeadLetterStore;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.domain.model.ReplayOptions;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class DefaultEventBus implements EventBus {
    private static final Logger log = LoggerFactory.getLogger(DefaultEventBus.class);

    private final Publisher publisher;
    private final Subscriber subscriber;
    private final Serializer serializer;
    private final ReplayableStore replayableStore;
    private final DeadLetterStore deadLetterStore;
    private final AtomicLong offsetGenerator = new AtomicLong(0);

    // Track local subscribers to dispatch incoming messages correctly
    private final Map<String, List<Subscription<?>>> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, Boolean> subscribedTopics = new ConcurrentHashMap<>();

    public DefaultEventBus(Publisher publisher, Subscriber subscriber, Serializer serializer) {
        this(publisher, subscriber, serializer, null, null);
    }

    public DefaultEventBus(Publisher publisher, Subscriber subscriber, Serializer serializer,
                           ReplayableStore replayableStore, DeadLetterStore deadLetterStore) {
        this.publisher = publisher;
        this.subscriber = subscriber;
        this.serializer = serializer;
        this.replayableStore = replayableStore;
        this.deadLetterStore = deadLetterStore;
    }

    @Override
    public void publish(String topic, Object event) {
        publish(topic, event, Map.of());
    }

    @Override
    public void publish(String topic, Object event, Map<String, String> headers) {
        byte[] payload = serializer.serialize(event);
        String payloadType = event.getClass().getName();
        Event envelope = new Event(
                UUID.randomUUID().toString(),
                topic,
                payload,
                payloadType,
                System.currentTimeMillis(),
                0L, // placeholder offset
                headers
        );

        if (replayableStore != null) {
            try {
                replayableStore.save(envelope); // This generates/sets the persistent offset
            } catch (Exception e) {
                log.error("Failed to save event {} to ReplayableStore", envelope.getId(), e);
                throw new RuntimeException("Persistence failure", e);
            }
        } else {
            envelope.setOffset(offsetGenerator.incrementAndGet());
        }

        log.debug("Publishing event {} to topic {}", envelope.getId(), topic);
        publisher.publish(envelope);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void subscribe(String topic, Class<T> eventType, Consumer<T> consumer) {
        log.info("Registering subscriber for topic: {} with event type: {}", topic, eventType.getSimpleName());
        subscriptions.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>())
                .add(new Subscription<>(eventType, consumer));

        // Register callback with the SPI subscriber adapter if not already done
        subscribedTopics.computeIfAbsent(topic, t -> {
            subscriber.subscribe(t, this::routeEvent);
            return true;
        });
    }

    @Override
    public void replay(String topic, ReplayOptions options) {
        if (replayableStore == null) {
            log.warn("Replay is not supported by the current event bus provider configuration for topic: {}", topic);
            return;
        }

        log.info("Starting replay for topic: {} with options: {}", topic, options);
        List<Event> historicalEvents = switch (options.type()) {
            case ALL -> replayableStore.findByTopic(topic);
            case FROM_TIMESTAMP -> replayableStore.findByTopicFromTimestamp(topic, options.timestamp());
            case FROM_OFFSET -> replayableStore.findByTopicFromOffset(topic, options.offset());
        };

        if (historicalEvents == null || historicalEvents.isEmpty()) {
            log.debug("No historical events found to replay for topic: {}", topic);
            return;
        }

        // Deliver sequentially to subscribers of this topic
        for (Event event : historicalEvents) {
            routeEvent(event);
        }
    }

    public java.util.Set<String> getSubscribedTopics() {
        return subscriptions.keySet();
    }

    private void routeEvent(Event event) {
        List<Subscription<?>> topicSubscriptions = subscriptions.get(event.getTopic());
        if (topicSubscriptions == null || topicSubscriptions.isEmpty()) {
            log.debug("No active subscriptions found for topic: {}", event.getTopic());
            return;
        }

        for (Subscription<?> sub : topicSubscriptions) {
            try {
                if (sub.eventType.getName().equals(event.getPayloadType())) {
                    Object deserializedPayload = serializer.deserialize(event.getPayload(), sub.eventType);
                    invokeHandler(sub.consumer, deserializedPayload);
                }
            } catch (Exception e) {
                log.error("Failed to route event {} to subscriber of type {}", event.getId(), sub.eventType.getName(), e);
                if (deadLetterStore != null) {
                    try {
                        deadLetterStore.saveDeadLetter(event, e);
                    } catch (Exception dlqEx) {
                        log.error("Failed to write event {} to Dead Letter Store", event.getId(), dlqEx);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void invokeHandler(Consumer<T> consumer, Object payload) {
        consumer.accept((T) payload);
    }

    private record Subscription<T>(Class<T> eventType, Consumer<T> consumer) {}
}
