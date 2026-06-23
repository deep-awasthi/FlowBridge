package org.flowbridge.starter.annotation;

import java.lang.annotation.*;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface FlowBridgeListener {
    /**
     * The topic name to subscribe to.
     *
     * @return the topic name
     */
    String topic();
}
