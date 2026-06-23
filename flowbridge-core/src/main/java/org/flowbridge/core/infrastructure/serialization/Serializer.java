package org.flowbridge.core.infrastructure.serialization;

public interface Serializer {

    /**
     * Serializes an object to a byte array.
     *
     * @param object the object to serialize
     * @param <T>    the type of the object
     * @return the serialized byte array
     */
    <T> byte[] serialize(T object);

    /**
     * Deserializes a byte array back to an object of the specified class.
     *
     * @param data  the serialized byte array
     * @param clazz the target class of the object
     * @param <T>   the type of the object
     * @return the deserialized object
     */
    <T> T deserialize(byte[] data, Class<T> clazz);
}
