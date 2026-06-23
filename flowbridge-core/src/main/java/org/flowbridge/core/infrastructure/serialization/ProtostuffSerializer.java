package org.flowbridge.core.infrastructure.serialization;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtostuffSerializer implements Serializer {

    private final Map<Class<?>, Schema<?>> schemaCache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private <T> Schema<T> getSchema(Class<T> clazz) {
        return (Schema<T>) schemaCache.computeIfAbsent(clazz, RuntimeSchema::getSchema);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> byte[] serialize(T object) {
        if (object == null) {
            return new byte[0];
        }
        Class<T> clazz = (Class<T>) object.getClass();
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        try {
            Schema<T> schema = getSchema(clazz);
            return ProtostuffIOUtil.toByteArray(object, schema, buffer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object of type: " + clazz.getName(), e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> clazz) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            Schema<T> schema = getSchema(clazz);
            T message = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(data, message, schema);
            return message;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize object of type: " + clazz.getName(), e);
        }
    }
}
