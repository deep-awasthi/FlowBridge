package org.flowbridge.embedded;

import org.flowbridge.core.application.port.out.Publisher;
import org.flowbridge.core.domain.model.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class EmbeddedPublisher implements Publisher {
    private static final Logger log = LoggerFactory.getLogger(EmbeddedPublisher.class);

    private final EmbeddedEventRegistry registry;
    private final ExecutorService executor;

    public EmbeddedPublisher(EmbeddedEventRegistry registry) {
        this.registry = registry;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void publish(Event event) {
        List<Consumer<Event>> consumers = registry.getListeners(event.getTopic());
        if (consumers.isEmpty()) {
            log.debug("Embedded bus: No subscribers registered for topic '{}'", event.getTopic());
            return;
        }

        for (Consumer<Event> consumer : consumers) {
            executor.submit(() -> {
                try {
                    consumer.accept(event);
                } catch (Exception e) {
                    log.error("Embedded bus error: Subscriber execution failed for topic '{}'", event.getTopic(), e);
                }
            });
        }
    }

    public void close() {
        executor.shutdown();
    }
}
