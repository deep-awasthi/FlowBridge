package org.flowbridge.core.infrastructure.serialization;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtostuffSerializerTest {

    private final Serializer serializer = new ProtostuffSerializer();

    public static class TestPayload {
        private String orderId;
        private double amount;
        private String status;

        public TestPayload() {
        }

        public TestPayload(String orderId, double amount, String status) {
            this.orderId = orderId;
            this.amount = amount;
            this.status = status;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public double getAmount() {
            return amount;
        }

        public void setAmount(double amount) {
            this.amount = amount;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    @Test
    public void testSerializationAndDeserialization() {
        // Arrange
        TestPayload original = new TestPayload("order-100", 129.50, "CREATED");

        // Act
        byte[] serializedData = serializer.serialize(original);
        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestPayload deserialized = serializer.deserialize(serializedData, TestPayload.class);

        // Assert
        assertNotNull(deserialized);
        assertEquals(original.getOrderId(), deserialized.getOrderId());
        assertEquals(original.getAmount(), deserialized.getAmount());
        assertEquals(original.getStatus(), deserialized.getStatus());
    }
}
