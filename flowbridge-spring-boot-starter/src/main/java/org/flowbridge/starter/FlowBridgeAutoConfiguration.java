package org.flowbridge.starter;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.port.out.DeadLetterStore;
import org.flowbridge.core.application.port.out.Publisher;
import org.flowbridge.core.application.port.out.ReplayableStore;
import org.flowbridge.core.application.port.out.Subscriber;
import org.flowbridge.core.application.service.DefaultEventBus;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.flowbridge.local.LocalEventRegistry;
import org.flowbridge.local.LocalPublisher;
import org.flowbridge.local.LocalSubscriber;
import org.flowbridge.embedded.*;
import org.flowbridge.kafka.KafkaConsumerLoop;
import org.flowbridge.kafka.KafkaEventRegistry;
import org.flowbridge.kafka.KafkaPublisher;
import org.flowbridge.kafka.KafkaSubscriber;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(FlowBridgeProperties.class)
public class FlowBridgeAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Serializer serializer() {
        return new ProtostuffSerializer();
    }

    @Bean
    @ConditionalOnMissingBean
    public static FlowBridgeListenerBeanPostProcessor flowBridgeListenerBeanPostProcessor(EventBus eventBus) {
        return new FlowBridgeListenerBeanPostProcessor(eventBus);
    }

    @Configuration
    @ConditionalOnProperty(name = "flowbridge.provider", havingValue = "local", matchIfMissing = true)
    public static class LocalProviderConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public LocalEventRegistry localEventRegistry() {
            return new LocalEventRegistry();
        }

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        public Publisher localPublisher(LocalEventRegistry registry) {
            return new LocalPublisher(registry);
        }

        @Bean
        @ConditionalOnMissingBean
        public Subscriber localSubscriber(LocalEventRegistry registry) {
            return new LocalSubscriber(registry);
        }

        @Bean
        @ConditionalOnMissingBean
        public EventBus eventBus(Publisher publisher, Subscriber subscriber, Serializer serializer) {
            return new DefaultEventBus(publisher, subscriber, serializer);
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "flowbridge.provider", havingValue = "embedded")
    public static class EmbeddedProviderConfiguration {

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        public RocksDBManager rocksDBManager(FlowBridgeProperties properties) {
            return new RocksDBManager(properties.getEmbedded().getDataDir());
        }

        @Bean
        @ConditionalOnMissingBean
        public ReplayableStore replayableStore(RocksDBManager manager, Serializer serializer) {
            return new RocksDBReplayableStore(manager, serializer);
        }

        @Bean
        @ConditionalOnMissingBean
        public DeadLetterStore deadLetterStore(RocksDBManager manager, Serializer serializer) {
            return new RocksDBDeadLetterStore(manager, serializer);
        }

        @Bean
        @ConditionalOnMissingBean
        public EmbeddedEventRegistry embeddedEventRegistry() {
            return new EmbeddedEventRegistry();
        }

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        public Publisher embeddedPublisher(EmbeddedEventRegistry registry) {
            return new EmbeddedPublisher(registry);
        }

        @Bean
        @ConditionalOnMissingBean
        public Subscriber embeddedSubscriber(EmbeddedEventRegistry registry) {
            return new EmbeddedSubscriber(registry);
        }

        @Bean
        @ConditionalOnMissingBean
        public EventBus eventBus(Publisher publisher, Subscriber subscriber, Serializer serializer,
                                 ReplayableStore replayableStore, DeadLetterStore deadLetterStore) {
            return new DefaultEventBus(publisher, subscriber, serializer, replayableStore, deadLetterStore);
        }
    }

    @Configuration
    @ConditionalOnProperty(name = "flowbridge.provider", havingValue = "kafka")
    @ConditionalOnClass(name = "org.apache.kafka.clients.producer.KafkaProducer")
    public static class KafkaProviderConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public KafkaEventRegistry kafkaEventRegistry() {
            return new KafkaEventRegistry();
        }

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        public Publisher kafkaPublisher(FlowBridgeProperties properties) {
            return new KafkaPublisher(
                    properties.getKafka().getBootstrapServers(),
                    properties.getKafka().getTopicPrefix()
            );
        }

        @Bean
        @ConditionalOnMissingBean
        public Subscriber kafkaSubscriber(KafkaEventRegistry registry, FlowBridgeProperties properties) {
            return new KafkaSubscriber(registry, properties.getKafka().getTopicPrefix());
        }

        /**
         * Starts the Kafka consumer polling loop on a virtual thread.
         * The loop is closed (via {@link KafkaConsumerLoop#close()}) when the Spring context shuts down.
         */
        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        public KafkaConsumerLoop kafkaConsumerLoop(KafkaEventRegistry registry,
                                                   FlowBridgeProperties properties) {
            KafkaConsumerLoop loop = new KafkaConsumerLoop(
                    properties.getKafka().getBootstrapServers(),
                    properties.getKafka().getGroupId(),
                    registry
            );
            loop.startOnVirtualThread();
            return loop;
        }

        @Bean
        @ConditionalOnMissingBean
        public EventBus eventBus(Publisher publisher, Subscriber subscriber, Serializer serializer) {
            return new DefaultEventBus(publisher, subscriber, serializer);
        }
    }
}
