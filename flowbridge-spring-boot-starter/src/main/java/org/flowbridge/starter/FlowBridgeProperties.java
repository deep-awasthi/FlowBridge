package org.flowbridge.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "flowbridge")
public class FlowBridgeProperties {
    
    private ProviderType provider = ProviderType.LOCAL;
    private final EmbeddedProperties embedded = new EmbeddedProperties();
    private final KafkaProperties kafka = new KafkaProperties();

    public enum ProviderType {
        LOCAL,
        EMBEDDED,
        KAFKA
    }

    public static class EmbeddedProperties {
        private String dataDir = "./data/flowbridge";

        public String getDataDir() {
            return dataDir;
        }

        public void setDataDir(String dataDir) {
            this.dataDir = dataDir;
        }
    }

    public static class KafkaProperties {
        private String bootstrapServers = "localhost:9092";
        private String groupId = "flowbridge-group";
        private String topicPrefix = "";

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getTopicPrefix() {
            return topicPrefix;
        }

        public void setTopicPrefix(String topicPrefix) {
            this.topicPrefix = topicPrefix;
        }
    }

    public ProviderType getProvider() {
        return provider;
    }

    public void setProvider(ProviderType provider) {
        this.provider = provider;
    }

    public EmbeddedProperties getEmbedded() {
        return embedded;
    }

    public KafkaProperties getKafka() {
        return kafka;
    }
}
