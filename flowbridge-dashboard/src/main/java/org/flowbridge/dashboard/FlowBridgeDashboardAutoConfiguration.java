package org.flowbridge.dashboard;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.port.out.DeadLetterStore;
import org.flowbridge.core.application.port.out.ReplayableStore;
import org.flowbridge.core.infrastructure.serialization.Serializer;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration that registers the FlowBridge web dashboard when:
 * <ul>
 *   <li>A servlet web application is present (spring-boot-starter-web is on the classpath), and</li>
 *   <li>Thymeleaf's {@code TemplateEngine} is available (spring-boot-starter-thymeleaf is on the classpath).</li>
 * </ul>
 *
 * <p>Both stores are optional; the dashboard degrades gracefully when running
 * under the {@code local} provider (no persistence layer).
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(name = "org.thymeleaf.TemplateEngine")
public class FlowBridgeDashboardAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public FlowBridgeDashboardController flowBridgeDashboardController(
            EventBus eventBus,
            Serializer serializer,
            @org.springframework.beans.factory.annotation.Autowired(required = false) ReplayableStore replayableStore,
            @org.springframework.beans.factory.annotation.Autowired(required = false) DeadLetterStore deadLetterStore) {

        return new FlowBridgeDashboardController(eventBus, serializer, replayableStore, deadLetterStore);
    }
}
