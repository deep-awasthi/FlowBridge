package org.flowbridge.examples;

import org.flowbridge.starter.annotation.FlowBridgeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Simulates an inventory management system that decrements stock when an order is placed.
 *
 * <p>Subscribes to the {@code orders.placed} topic via the {@link FlowBridgeListener} annotation.
 * The framework automatically binds this method to the event bus at startup — no manual
 * subscription code required.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    /**
     * Handles an {@link OrderEvent} when a new order is placed.
     * Simulates decrementing inventory stock for the purchased item.
     *
     * @param event the order event deserialized from the event bus
     */
    @FlowBridgeListener(topic = "orders.placed")
    public void onOrderPlaced(OrderEvent event) {
        log.info("[InventoryService] ✅ Reserving stock for order {} (customer={}, amount={})",
                event.getOrderId(), event.getCustomerId(), event.getAmount());

        // Simulate inventory update latency
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("[InventoryService] Stock reserved successfully for order {}", event.getOrderId());
    }
}
