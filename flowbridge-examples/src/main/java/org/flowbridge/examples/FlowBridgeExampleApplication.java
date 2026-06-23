package org.flowbridge.examples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * FlowBridge Order Processing Example.
 *
 * <p>This application demonstrates FlowBridge as a lightweight event bus in a realistic
 * order-processing scenario with two active consumers and a DLQ demo.
 *
 * <p>Run with different providers via Spring profiles:
 * <pre>
 *   # In-memory (default)
 *   java -jar flowbridge-examples.jar
 *
 *   # Embedded RocksDB persistence + replay + DLQ
 *   java -jar flowbridge-examples.jar --spring.profiles.active=embedded
 * </pre>
 *
 * Dashboard is available at: <a href="http://localhost:8080/flowbridge">http://localhost:8080/flowbridge</a>
 */
@SpringBootApplication
public class FlowBridgeExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlowBridgeExampleApplication.class, args);
    }
}
