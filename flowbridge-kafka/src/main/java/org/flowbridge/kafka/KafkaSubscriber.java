package org.flowbridge.kafka;

import org.flowbridge.core.application.port.out.Subscriber;
import org.flowbridge.core.domain.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * {@link Subscriber} adapter that registers event callbacks into the {@link KafkaEventRegistry}.
 *
 * <p>The actual Kafka consumption loop is handled by {@link KafkaConsumerLoop}, which polls
 * Kafka and dispatches received events through the registry.
 */
public class KafkaSubscriber implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(KafkaSubscriber.class);

    private final KafkaEventRegistry registry;
    private final String topicPrefix;

    public KafkaSubscriber(KafkaEventRegistry registry, String topicPrefix) {
        this.registry    = registry;
        this.topicPrefix = topicPrefix == null ? "" : topicPrefix;
    }

    @Override
    public void subscribe(String topic, Consumer<Event> callback) {
        String kafkaTopic = topicPrefix.isEmpty() ? topic : topicPrefix + "." + topic;
        log.info("Registering Kafka subscriber for topic '{}'", kafkaTopic);
        registry.register(kafkaTopic, callback);
    }
}
