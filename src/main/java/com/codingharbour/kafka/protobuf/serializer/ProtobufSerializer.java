package com.codingharbour.kafka.protobuf.serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ProtobufSerializer <T extends com.google.protobuf.GeneratedMessageV3> implements Serializer<T> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Optional configuration
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray();  // Serialize Protobuf to byte array
    }

    @Override
    public void close() {
        // Cleanup if needed
    }
}