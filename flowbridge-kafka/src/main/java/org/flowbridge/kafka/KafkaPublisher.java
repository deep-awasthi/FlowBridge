package org.flowbridge.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.flowbridge.core.application.port.out.Publisher;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * {@link Publisher} adapter that sends serialized {@link Event} bytes to an Apache Kafka topic.
 *
 * <p>Each call to {@link #publish(Event)} sends the event as a Kafka message where:
 * <ul>
 *   <li>Key  = {@code event.getId()} (enables per-event ordering within a partition)</li>
 *   <li>Value = the pre-serialized event envelope bytes produced by the eventSerializer</li>
 * </ul>
 *
 * <p>The publisher is {@link AutoCloseable} and must be closed during application shutdown
 * to flush pending messages and release Kafka producer connections.
 */
public class KafkaPublisher implements Publisher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String topicPrefix;
    private final ProtostuffSerializer eventSerializer;

    public KafkaPublisher(String bootstrapServers, String topicPrefix) {
        this.topicPrefix = topicPrefix == null ? "" : topicPrefix;
        this.eventSerializer = new ProtostuffSerializer();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Reliable delivery: wait for all in-sync replicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Idempotent producer (exactly-once semantics within a session)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Retry on transient failures
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
        log.info("KafkaPublisher created (bootstrap={}, prefix='{}')", bootstrapServers, this.topicPrefix);
    }

    @Override
    public void publish(Event event) {
        String kafkaTopic = topicPrefix.isEmpty() ? event.getTopic() : topicPrefix + "." + event.getTopic();
        byte[] serializedEvent = eventSerializer.serialize(event);

        producer.send(
                new ProducerRecord<>(kafkaTopic, event.getId(), serializedEvent),
                (metadata, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event {} to Kafka topic '{}': {}",
                                event.getId(), kafkaTopic, ex.getMessage(), ex);
                    } else {
                        log.debug("Published event {} to Kafka topic '{}' partition={} offset={}",
                                event.getId(), kafkaTopic, metadata.partition(), metadata.offset());
                    }
                }
        );
    }

    @Override
    public void close() {
        log.info("Closing KafkaPublisher — flushing pending messages...");
        producer.flush();
        producer.close();
        log.info("KafkaPublisher closed.");
    }
}
