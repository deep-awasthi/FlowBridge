package org.flowbridge.embedded;

import org.flowbridge.core.application.port.out.Subscriber;
import org.flowbridge.core.domain.model.Event;
import java.util.function.Consumer;

public class EmbeddedSubscriber implements Subscriber {
    private final EmbeddedEventRegistry registry;

    public EmbeddedSubscriber(EmbeddedEventRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void subscribe(String topic, Consumer<Event> consumer) {
        registry.register(topic, consumer);
    }
}
