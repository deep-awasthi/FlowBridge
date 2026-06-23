package org.flowbridge.dashboard;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.port.out.DeadLetterStore;
import org.flowbridge.core.application.port.out.ReplayableStore;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.flowbridge.embedded.RocksDBDeadLetterStore;
import org.flowbridge.embedded.RocksDBManager;
import org.flowbridge.embedded.RocksDBReplayableStore;
import org.flowbridge.local.LocalEventRegistry;
import org.flowbridge.local.LocalPublisher;
import org.flowbridge.local.LocalSubscriber;
import org.flowbridge.core.application.service.DefaultEventBus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;
import org.springframework.web.servlet.mvc.support.RedirectAttributesModelMap;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link FlowBridgeDashboardController}.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Dashboard rendering with local (no persistence) provider</li>
 *   <li>Dashboard rendering with embedded (RocksDB) provider</li>
 *   <li>DLQ retry endpoint – requeues the event onto the bus and removes the DLQ entry</li>
 *   <li>DLQ delete endpoint – removes DLQ entry without re-publishing</li>
 *   <li>Replay endpoint with all three replay types (ALL, FROM_OFFSET, FROM_TIMESTAMP)</li>
 * </ul>
 */
@DisplayName("FlowBridge Dashboard Controller Tests")
class FlowBridgeDashboardTest {

    // ── Shared fixtures ────────────────────────────────────────────────────────

    @TempDir
    Path tempDir;

    private Serializer serializer;

    // Embedded-mode fixtures (RocksDB)
    private RocksDBManager rocksDBManager;
    private RocksDBReplayableStore replayableStore;
    private RocksDBDeadLetterStore deadLetterStore;

    // Local-mode fixtures
    private LocalEventRegistry localRegistry;
    private LocalPublisher localPublisher;
    private LocalSubscriber localSubscriber;

    // Payload class used across all tests
    public static class OrderEvent {
        private String orderId;
        public OrderEvent() {}
        public OrderEvent(String orderId) { this.orderId = orderId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
    }

    @BeforeEach
    void setUp() {
        serializer = new ProtostuffSerializer();
        rocksDBManager  = new RocksDBManager(tempDir.resolve("rocksdb").toString());
        replayableStore = new RocksDBReplayableStore(rocksDBManager, serializer);
        deadLetterStore = new RocksDBDeadLetterStore(rocksDBManager, serializer);

        localRegistry  = new LocalEventRegistry();
        localPublisher = new LocalPublisher(localRegistry);
        localSubscriber = new LocalSubscriber(localRegistry);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (localPublisher != null) localPublisher.close();
        if (rocksDBManager != null)  rocksDBManager.close();
    }

    // ── Helper: build EventBus in embedded mode ────────────────────────────────

    private DefaultEventBus buildEmbeddedBus() {
        var embeddedRegistry   = new org.flowbridge.embedded.EmbeddedEventRegistry();
        var embeddedPublisher  = new org.flowbridge.embedded.EmbeddedPublisher(embeddedRegistry);
        var embeddedSubscriber = new org.flowbridge.embedded.EmbeddedSubscriber(embeddedRegistry);
        return new DefaultEventBus(embeddedPublisher, embeddedSubscriber, serializer,
                replayableStore, deadLetterStore);
    }

    // ── Helper: build EventBus in local mode ──────────────────────────────────

    private DefaultEventBus buildLocalBus() {
        return new DefaultEventBus(localPublisher, localSubscriber, serializer);
    }

    // ── Helper: build controller under test ───────────────────────────────────

    private FlowBridgeDashboardController controllerWithEmbedded() {
        return new FlowBridgeDashboardController(
                buildEmbeddedBus(), serializer, replayableStore, deadLetterStore);
    }

    private FlowBridgeDashboardController controllerWithLocal() {
        return new FlowBridgeDashboardController(
                buildLocalBus(), serializer, null, null);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // showDashboard
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("showDashboard – local provider returns empty statistics with correct provider label")
    void showDashboard_localProvider_hasCorrectModelAttributes() {
        FlowBridgeDashboardController controller = controllerWithLocal();
        Model model = new ExtendedModelMap();

        String view = controller.showDashboard(model);

        assertEquals("flowbridge/dashboard", view);
        assertEquals(0, model.getAttribute("totalTopics"));
        assertEquals(0L, model.getAttribute("totalMessages"));
        assertEquals(0, ((List<?>) model.getAttribute("dlqRecords")).size());
        assertEquals("Local (In-Memory)", model.getAttribute("provider"));
    }

    @Test
    @DisplayName("showDashboard – embedded provider shows persisted topic and message count")
    void showDashboard_embeddedProvider_reflectsPersistedData() throws Exception {
        DefaultEventBus bus = buildEmbeddedBus();
        CountDownLatch latch = new CountDownLatch(1);
        bus.subscribe("orders", OrderEvent.class, e -> latch.countDown());
        bus.publish("orders", new OrderEvent("ORD-001"));
        latch.await(3, TimeUnit.SECONDS);

        FlowBridgeDashboardController controller = new FlowBridgeDashboardController(
                bus, serializer, replayableStore, deadLetterStore);
        Model model = new ExtendedModelMap();
        controller.showDashboard(model);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> topics = (List<Map<String, Object>>) model.getAttribute("topics");

        assertThat(topics).isNotEmpty();
        Map<String, Object> ordersTopic = topics.stream()
                .filter(t -> "orders".equals(t.get("name")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("'orders' topic not found in dashboard model"));

        assertEquals(true,  ordersTopic.get("isSubscribed"));
        assertEquals(true,  ordersTopic.get("isPersisted"));
        assertThat((Long) ordersTopic.get("messageCount")).isGreaterThanOrEqualTo(1L);
        assertEquals("Embedded (RocksDB)", model.getAttribute("provider"));
    }

    @Test
    @DisplayName("showDashboard – subscribed topics (local) are listed even with no persistence")
    void showDashboard_subscribedTopics_appearsInList() throws Exception {
        DefaultEventBus bus = buildLocalBus();
        CountDownLatch latch = new CountDownLatch(1);
        bus.subscribe("payments", OrderEvent.class, e -> latch.countDown());
        bus.publish("payments", new OrderEvent("PAY-001"));
        latch.await(2, TimeUnit.SECONDS);

        FlowBridgeDashboardController controller = new FlowBridgeDashboardController(
                bus, serializer, null, null);
        Model model = new ExtendedModelMap();
        controller.showDashboard(model);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> topics = (List<Map<String, Object>>) model.getAttribute("topics");

        boolean hasPayments = topics.stream().anyMatch(t -> "payments".equals(t.get("name")));
        assertTrue(hasPayments, "Subscribed topic 'payments' should appear in dashboard model");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DLQ retry
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("retryDlq – re-publishes event and removes it from the DLQ")
    void retryDlq_republishesAndCleansUp() throws Exception {
        DefaultEventBus bus = buildEmbeddedBus();
        CountDownLatch deliveryLatch = new CountDownLatch(1);
        bus.subscribe("orders", OrderEvent.class, e -> deliveryLatch.countDown());

        // Manually place an event into the DLQ (simulate consumer failure)
        byte[] payload = serializer.serialize(new OrderEvent("ORD-DLQ-001"));
        Event failedEvent = new Event(
                "dlq-test-event-001",
                "orders",
                payload,
                OrderEvent.class.getName(),
                System.currentTimeMillis(),
                0L,
                new HashMap<>());
        deadLetterStore.saveDeadLetter(failedEvent, new RuntimeException("Simulated failure"));

        assertThat(deadLetterStore.findAll()).hasSize(1);

        FlowBridgeDashboardController controller = new FlowBridgeDashboardController(
                bus, serializer, replayableStore, deadLetterStore);
        var redirectAttrs = new RedirectAttributesModelMap();

        String redirect = controller.retryDlq("dlq-test-event-001", redirectAttrs);

        assertEquals("redirect:/flowbridge", redirect);
        assertThat(redirectAttrs.getFlashAttributes()).containsKey("success");
        // DLQ must be empty after retry
        assertThat(deadLetterStore.findAll()).isEmpty();
        // Consumer must have received the re-published event
        assertTrue(deliveryLatch.await(3, TimeUnit.SECONDS),
                "Re-published event was not delivered to subscriber");
    }

    @Test
    @DisplayName("retryDlq – returns error flash when event not found in DLQ")
    void retryDlq_eventNotFound_flashesError() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        String redirect = controller.retryDlq("non-existent-id", redirectAttrs);

        assertEquals("redirect:/flowbridge", redirect);
        assertThat(redirectAttrs.getFlashAttributes()).containsKey("error");
    }

    @Test
    @DisplayName("retryDlq – returns error flash when DLQ store is not configured (local mode)")
    void retryDlq_noDlqStore_flashesError() {
        FlowBridgeDashboardController controller = controllerWithLocal();
        var redirectAttrs = new RedirectAttributesModelMap();

        String redirect = controller.retryDlq("any-id", redirectAttrs);

        assertEquals("redirect:/flowbridge", redirect);
        assertThat((String) redirectAttrs.getFlashAttributes().get("error"))
                .contains("not configured");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // DLQ delete
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("deleteDlq – removes DLQ entry without re-publishing")
    void deleteDlq_removesEntry() throws Exception {
        byte[] payload = serializer.serialize(new OrderEvent("ORD-DELETE-001"));
        Event e = new Event(
                "dlq-delete-001",
                "orders",
                payload,
                OrderEvent.class.getName(),
                System.currentTimeMillis(),
                0L,
                new HashMap<>());
        deadLetterStore.saveDeadLetter(e, new RuntimeException("Error"));

        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        String redirect = controller.deleteDlq("dlq-delete-001", redirectAttrs);

        assertEquals("redirect:/flowbridge", redirect);
        assertThat(redirectAttrs.getFlashAttributes()).containsKey("success");
        assertThat(deadLetterStore.findAll()).isEmpty();
    }

    @Test
    @DisplayName("deleteDlq – returns error flash when DLQ store is not configured (local mode)")
    void deleteDlq_noDlqStore_flashesError() {
        FlowBridgeDashboardController controller = controllerWithLocal();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.deleteDlq("any-id", redirectAttrs);

        assertThat((String) redirectAttrs.getFlashAttributes().get("error"))
                .contains("not configured");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Replay
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    @DisplayName("triggerReplay – ALL type replays stored events to subscribers")
    void triggerReplay_allType_redeliversEvents() throws Exception {
        DefaultEventBus bus = buildEmbeddedBus();

        // Publish two events and wait for them to persist
        CountDownLatch publishLatch = new CountDownLatch(2);
        bus.subscribe("invoices", OrderEvent.class, e -> publishLatch.countDown());
        bus.publish("invoices", new OrderEvent("INV-001"));
        bus.publish("invoices", new OrderEvent("INV-002"));
        publishLatch.await(3, TimeUnit.SECONDS);

        // Now register a fresh subscriber to capture replays
        CountDownLatch replayLatch = new CountDownLatch(2);
        bus.subscribe("invoices", OrderEvent.class, e -> replayLatch.countDown());

        FlowBridgeDashboardController controller = new FlowBridgeDashboardController(
                bus, serializer, replayableStore, deadLetterStore);
        var redirectAttrs = new RedirectAttributesModelMap();

        String redirect = controller.triggerReplay("invoices", "ALL", null, null, redirectAttrs);

        assertEquals("redirect:/flowbridge", redirect);
        assertThat(redirectAttrs.getFlashAttributes()).containsKey("success");
        assertTrue(replayLatch.await(4, TimeUnit.SECONDS),
                "Replayed events were not re-delivered to new subscriber");
    }

    @Test
    @DisplayName("triggerReplay – FROM_OFFSET without offset returns error flash")
    void triggerReplay_fromOffsetWithoutOffset_flashesError() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "FROM_OFFSET", null, null, redirectAttrs);

        assertThat((String) redirectAttrs.getFlashAttributes().get("error"))
                .contains("Offset must be specified");
    }

    @Test
    @DisplayName("triggerReplay – FROM_TIMESTAMP without timestamp returns error flash")
    void triggerReplay_fromTimestampWithoutTimestamp_flashesError() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "FROM_TIMESTAMP", null, null, redirectAttrs);

        assertThat((String) redirectAttrs.getFlashAttributes().get("error"))
                .contains("Timestamp must be specified");
    }

    @Test
    @DisplayName("triggerReplay – FROM_TIMESTAMP with invalid format returns error flash")
    void triggerReplay_fromTimestampInvalidFormat_flashesError() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "FROM_TIMESTAMP", null, "not-a-date", redirectAttrs);

        assertThat((String) redirectAttrs.getFlashAttributes().get("error"))
                .contains("Invalid timestamp format");
    }

    @Test
    @DisplayName("triggerReplay – FROM_TIMESTAMP with valid ISO instant succeeds")
    void triggerReplay_fromTimestampIso_succeeds() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "FROM_TIMESTAMP", null,
                "2020-01-01T00:00:00Z", redirectAttrs);

        // No error flash – replay was triggered (even if 0 events match the filter)
        assertThat(redirectAttrs.getFlashAttributes()).doesNotContainKey("error");
    }

    @Test
    @DisplayName("triggerReplay – FROM_TIMESTAMP with epoch-millis string succeeds")
    void triggerReplay_fromTimestampEpochMillis_succeeds() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "FROM_TIMESTAMP", null,
                "0", redirectAttrs);

        assertThat(redirectAttrs.getFlashAttributes()).doesNotContainKey("error");
    }

    @Test
    @DisplayName("triggerReplay – FROM_OFFSET with valid offset succeeds")
    void triggerReplay_fromOffset_succeeds() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "FROM_OFFSET", 0L, null, redirectAttrs);

        assertThat(redirectAttrs.getFlashAttributes()).doesNotContainKey("error");
    }

    @Test
    @DisplayName("triggerReplay – unknown replay type returns error flash")
    void triggerReplay_unknownType_flashesError() {
        FlowBridgeDashboardController controller = controllerWithEmbedded();
        var redirectAttrs = new RedirectAttributesModelMap();

        controller.triggerReplay("invoices", "INVALID_TYPE", null, null, redirectAttrs);

        assertThat((String) redirectAttrs.getFlashAttributes().get("error"))
                .contains("Unknown replay type");
    }
}
