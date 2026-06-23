package org.flowbridge.examples;

import org.flowbridge.starter.annotation.FlowBridgeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * A deliberately failing event consumer used to demonstrate the Dead Letter Queue (DLQ).
 *
 * <p>When an event is published to the {@code orders.failing} topic:
 * <ol>
 *   <li>This listener receives and immediately throws a {@link RuntimeException}</li>
 *   <li>FlowBridge catches the exception, wraps it in a {@code DeadLetterRecord}, and
 *       persists it to the configured DLQ store (RocksDB in embedded mode)</li>
 *   <li>The failed event becomes visible on the dashboard at {@code /flowbridge}</li>
 *   <li>You can click <strong>Retry</strong> to re-publish the event or
 *       <strong>Discard</strong> to delete it</li>
 * </ol>
 *
 * <p>This pattern is useful for simulating transient downstream failures (e.g., a payment
 * gateway timeout) without losing the original event.
 *
 * <p><strong>Note</strong>: DLQ persistence only works with {@code flowbridge.provider=embedded}
 * or {@code kafka}. In local mode the failure is logged but not persisted.
 */
@Service
public class FailingService {

    private static final Logger log = LoggerFactory.getLogger(FailingService.class);

    @FlowBridgeListener(topic = "orders.failing")
    public void onFailingOrder(OrderEvent event) {
        log.warn("[FailingService] ⚠️  Processing order {} — this will intentionally fail to demonstrate DLQ",
                event.getOrderId());

        throw new RuntimeException(
                "Simulated downstream failure for order " + event.getOrderId() +
                " (e.g. payment gateway timeout). " +
                "Visit /flowbridge to retry or discard this event."
        );
    }
}
