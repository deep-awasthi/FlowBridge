package org.flowbridge.starter;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.local.LocalEventRegistry;
import org.flowbridge.embedded.RocksDBManager;
import org.flowbridge.starter.annotation.FlowBridgeListener;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class FlowBridgeStarterTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(FlowBridgeAutoConfiguration.class));

    public static class TestPayload {
        private String value;

        public TestPayload() {}

        public TestPayload(String value) {
            this.value = value;
        }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    @Configuration
    static class TestListenerConfiguration {
        @Bean
        public TestListener testListener() {
            return new TestListener();
        }
    }

    static class TestListener {
        final List<TestPayload> received = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);

        @FlowBridgeListener(topic = "starter-topic")
        public void onMessage(TestPayload payload) {
            received.add(payload);
            latch.countDown();
        }
    }

    @Test
    public void testLocalProviderConfiguredByDefault() {
        this.contextRunner.run((context) -> {
            assertThat(context).hasSingleBean(EventBus.class);
            assertThat(context).hasSingleBean(LocalEventRegistry.class);
            assertThat(context).doesNotHaveBean(RocksDBManager.class);
        });
    }

    @Test
    public void testLocalProviderConfiguredExplicitly() {
        this.contextRunner.withPropertyValues("flowbridge.provider=local")
                .run((context) -> {
                    assertThat(context).hasSingleBean(EventBus.class);
                    assertThat(context).hasSingleBean(LocalEventRegistry.class);
                    assertThat(context).doesNotHaveBean(RocksDBManager.class);
                });
    }

    @Test
    public void testEmbeddedProviderConfigured() {
        String tempDbPath = "./target/temp-starter-rocksdb-" + System.currentTimeMillis();
        
        this.contextRunner.withPropertyValues(
                "flowbridge.provider=embedded",
                "flowbridge.embedded.data-dir=" + tempDbPath
        ).run((context) -> {
            assertThat(context).hasSingleBean(EventBus.class);
            assertThat(context).hasSingleBean(RocksDBManager.class);
            assertThat(context).doesNotHaveBean(LocalEventRegistry.class);
        });
    }

    @Test
    public void testDeclarativeListenerInvoked() {
        this.contextRunner.withUserConfiguration(TestListenerConfiguration.class)
                .run((context) -> {
                    EventBus bus = context.getBean(EventBus.class);
                    TestListener listener = context.getBean(TestListener.class);
                    
                    bus.publish("starter-topic", new TestPayload("Declarative Hello!"));
                    
                    boolean finished = listener.latch.await(2, TimeUnit.SECONDS);
                    assertTrue(finished, "Annotation listener callback was not invoked");
                    assertEquals(1, listener.received.size());
                    assertEquals("Declarative Hello!", listener.received.get(0).getValue());
                });
    }
}
