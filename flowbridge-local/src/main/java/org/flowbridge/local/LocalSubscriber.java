package org.flowbridge.local;

import org.flowbridge.core.application.port.out.Subscriber;
import org.flowbridge.core.domain.model.Event;
import java.util.function.Consumer;

public class LocalSubscriber implements Subscriber {
    private final LocalEventRegistry registry;

    public LocalSubscriber(LocalEventRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void subscribe(String topic, Consumer<Event> consumer) {
        registry.register(topic, consumer);
    }
}
