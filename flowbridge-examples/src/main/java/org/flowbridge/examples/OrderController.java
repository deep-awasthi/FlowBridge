package org.flowbridge.examples;

import org.flowbridge.core.application.port.in.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

/**
 * REST controller that accepts order requests and publishes events onto FlowBridge.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>{@code POST /orders} – places a new order and publishes to {@code orders.placed}
 *   <li>{@code POST /orders/fail} – places an order on the {@code orders.failing} topic
 *       which always throws so the event lands in the Dead Letter Queue (DLQ)
 * </ul>
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private static final String TOPIC_PLACED  = "orders.placed";
    private static final String TOPIC_FAILING = "orders.failing";

    private final EventBus eventBus;

    public OrderController(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    /**
     * Place an order and broadcast the event to all registered consumers.
     *
     * @param request JSON body: {@code {"customerId":"C-001","amount":49.99}}
     * @return 202 Accepted with the generated order ID
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> placeOrder(@RequestBody Map<String, Object> request) {
        String orderId    = UUID.randomUUID().toString();
        String customerId = (String) request.getOrDefault("customerId", "UNKNOWN");
        double amount     = ((Number) request.getOrDefault("amount", 0)).doubleValue();

        OrderEvent event = new OrderEvent(orderId, customerId, amount, "PLACED");
        log.info("Publishing order event: {}", event);

        eventBus.publish(TOPIC_PLACED, event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId",  orderId,
                "topic",    TOPIC_PLACED,
                "status",   "PUBLISHED",
                "message",  "Order published. Check logs and /flowbridge for consumer activity."
        ));
    }

    /**
     * Publish an order to the {@code orders.failing} topic.
     * The {@link FailingService} listener always throws, so this event will end up in the DLQ.
     * Visit <a href="http://localhost:8080/flowbridge">the dashboard</a> to retry or discard it.
     *
     * @param request JSON body: same as {@code POST /orders}
     * @return 202 Accepted with the generated order ID and a hint about the DLQ
     */
    @PostMapping("/fail")
    public ResponseEntity<Map<String, Object>> placeFailingOrder(@RequestBody Map<String, Object> request) {
        String orderId    = UUID.randomUUID().toString();
        String customerId = (String) request.getOrDefault("customerId", "UNKNOWN");
        double amount     = ((Number) request.getOrDefault("amount", 0)).doubleValue();

        OrderEvent event = new OrderEvent(orderId, customerId, amount, "PLACED");
        log.info("Publishing order to failing topic: {}", event);

        eventBus.publish(TOPIC_FAILING, event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId",  orderId,
                "topic",    TOPIC_FAILING,
                "status",   "PUBLISHED",
                "message",  "Order published to failing topic. Visit /flowbridge to see it in the DLQ."
        ));
    }
}
