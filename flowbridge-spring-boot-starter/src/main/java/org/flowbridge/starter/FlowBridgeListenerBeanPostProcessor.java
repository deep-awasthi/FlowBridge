package org.flowbridge.starter;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.starter.annotation.FlowBridgeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

public class FlowBridgeListenerBeanPostProcessor implements BeanPostProcessor {
    private static final Logger log = LoggerFactory.getLogger(FlowBridgeListenerBeanPostProcessor.class);

    private final EventBus eventBus;

    public FlowBridgeListenerBeanPostProcessor(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = bean.getClass();
        ReflectionUtils.doWithMethods(targetClass, method -> {
            FlowBridgeListener annotation = AnnotationUtils.findAnnotation(method, FlowBridgeListener.class);
            if (annotation != null) {
                registerListener(bean, method, annotation);
            }
        });
        return bean;
    }

    private void registerListener(Object bean, Method method, FlowBridgeListener annotation) {
        String topic = annotation.topic();
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1) {
            throw new IllegalArgumentException("Method annotated with @FlowBridgeListener must have exactly one parameter: " 
                    + method.getDeclaringClass().getName() + "#" + method.getName());
        }
        
        Class<?> eventType = parameterTypes[0];
        log.info("Auto-registering @FlowBridgeListener on method {}#{} for topic '{}'",
                bean.getClass().getSimpleName(), method.getName(), topic);

        eventBus.subscribe(topic, eventType, payload -> {
            try {
                ReflectionUtils.makeAccessible(method);
                method.invoke(bean, payload);
            } catch (Exception e) {
                log.error("Failed to invoke listener method {}#{} for topic '{}'",
                        bean.getClass().getSimpleName(), method.getName(), topic, e);
                throw new RuntimeException("Listener invocation failed", e);
            }
        });
    }
}
