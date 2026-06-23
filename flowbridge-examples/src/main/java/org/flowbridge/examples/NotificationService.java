package org.flowbridge.examples;

import org.flowbridge.starter.annotation.FlowBridgeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Simulates a notification system that sends an email confirmation when an order is placed.
 *
 * <p>Subscribes to the same {@code orders.placed} topic as {@link InventoryService}.
 * FlowBridge delivers the event to both consumers independently and concurrently on
 * Virtual Threads — neither consumer blocks the other.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    /**
     * Handles an {@link OrderEvent} and sends a confirmation email to the customer.
     *
     * @param event the order event deserialized from the event bus
     */
    @FlowBridgeListener(topic = "orders.placed")
    public void onOrderPlaced(OrderEvent event) {
        log.info("[NotificationService] 📧 Sending confirmation email to customer {} for order {} (amount={})",
                event.getCustomerId(), event.getOrderId(), event.getAmount());

        // Simulate email dispatch latency
        try {
            Thread.sleep(80);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("[NotificationService] Email dispatched successfully for order {}", event.getOrderId());
    }
}
