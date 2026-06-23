package org.flowbridge.local;

import org.flowbridge.core.domain.model.Event;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class LocalEventRegistry {
    private final Map<String, List<Consumer<Event>>> listeners = new ConcurrentHashMap<>();

    public void register(String topic, Consumer<Event> listener) {
        listeners.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    public List<Consumer<Event>> getListeners(String topic) {
        return listeners.getOrDefault(topic, List.of());
    }
}
