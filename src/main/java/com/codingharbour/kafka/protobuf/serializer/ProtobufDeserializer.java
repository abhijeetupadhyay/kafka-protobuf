package com.codingharbour.kafka.protobuf.serializer;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ProtobufDeserializer<T extends com.google.protobuf.GeneratedMessageV3> implements Deserializer<T> {

    private Class<T> targetType;

    // No-arg constructor
    public ProtobufDeserializer() {
        // Default constructor for Kafka to instantiate
    }

    // Constructor to pass type
    public ProtobufDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Get target class from configs (if passed)
        String typeName = (String) configs.get("specific.protobuf.value.type");
        if (typeName != null) {
            try {
                this.targetType = (Class<T>) Class.forName(typeName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Protobuf class not found", e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return (T) targetType.getMethod("parseFrom", byte[].class).invoke(null, (Object) data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Protobuf message", e);
        }
    }

    @Override
    public void close() {
        // Cleanup if needed
    }
}