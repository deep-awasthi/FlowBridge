package org.flowbridge.examples;

/**
 * Event payload representing a placed order.
 *
 * <p>Must be a POJO with a no-arg constructor so Protostuff can serialize/deserialize it.
 */
public class OrderEvent {

    private String orderId;
    private String customerId;
    private double amount;
    private String status;

    /** Required by Protostuff serialization. */
    public OrderEvent() {}

    public OrderEvent(String orderId, String customerId, double amount, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    @Override
    public String toString() {
        return "OrderEvent{orderId='" + orderId + "', customerId='" + customerId +
               "', amount=" + amount + ", status='" + status + "'}";
    }
}
