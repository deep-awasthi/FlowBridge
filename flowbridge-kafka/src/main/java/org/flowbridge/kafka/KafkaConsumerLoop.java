package org.flowbridge.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background consumer loop that continuously polls Kafka for new messages
 * and dispatches them to registered subscribers via {@link KafkaEventRegistry}.
 *
 * <p>Runs on a dedicated virtual thread. Lifecycle:
 * <ol>
 *   <li>Created and started by {@code KafkaAutoConfiguration} on context refresh</li>
 *   <li>Polls the topics registered in the {@link KafkaEventRegistry}</li>
 *   <li>Gracefully stopped via {@link #close()} which triggers {@link KafkaConsumer#wakeup()}</li>
 * </ol>
 */
public class KafkaConsumerLoop implements AutoCloseable, Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLoop.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaEventRegistry registry;
    private final KafkaConsumer<String, byte[]> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ProtostuffSerializer eventSerializer;

    public KafkaConsumerLoop(String bootstrapServers, String groupId, KafkaEventRegistry registry) {
        this.registry = registry;
        this.eventSerializer = new ProtostuffSerializer();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // Start from the beginning when a new consumer group is created
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Manual offset commit for at-least-once delivery semantics
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        this.consumer = new KafkaConsumer<>(props);
    }

    /** Start the consumer loop on a new virtual thread. */
    public Thread startOnVirtualThread() {
        Thread thread = Thread.ofVirtual()
                .name("flowbridge-kafka-consumer")
                .start(this);
        log.info("KafkaConsumerLoop started on virtual thread '{}'", thread.getName());
        return thread;
    }

    @Override
    public void run() {
        running.set(true);
        try {
            // Subscribe to all topics currently registered in the registry
            Set<String> topics = registry.getTopics();
            if (topics.isEmpty()) {
                log.info("KafkaConsumerLoop: no topics registered yet — polling will begin when topics are added");
            } else {
                consumer.subscribe(new ArrayList<>(topics));
                log.info("KafkaConsumerLoop subscribed to topics: {}", topics);
            }

            while (running.get()) {
                // Re-subscribe if new topics were added after initial start
                Set<String> current = registry.getTopics();
                if (!current.isEmpty() && !current.equals(consumer.subscription())) {
                    consumer.subscribe(new ArrayList<>(current));
                    log.info("KafkaConsumerLoop re-subscribed to topics: {}", current);
                }

                ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
                records.forEach(record -> {
                    try {
                        byte[] data = record.value();
                        if (data != null && data.length > 0) {
                            Event event = eventSerializer.deserialize(data, Event.class);
                            if (event != null) {
                                registry.dispatch(record.topic(), event);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Failed to deserialize Event from Kafka topic '{}': {}", record.topic(), e.getMessage(), e);
                    }
                });
            }
        } catch (WakeupException e) {
            // Expected during graceful shutdown — do not log as error
            log.info("KafkaConsumerLoop received wakeup signal — shutting down");
        } catch (Exception e) {
            log.error("KafkaConsumerLoop encountered an unexpected error", e);
        } finally {
            consumer.close();
            log.info("KafkaConsumerLoop closed Kafka consumer.");
        }
    }

    @Override
    public void close() {
        log.info("Stopping KafkaConsumerLoop...");
        running.set(false);
        consumer.wakeup();  // Unblocks the poll() call
    }
}
