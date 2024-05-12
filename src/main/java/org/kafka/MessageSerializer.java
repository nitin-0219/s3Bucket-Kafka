package org.kafka;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageSerializer implements Serializer<Object> {

    public byte[] serialize(String topic, Object data) {
        byte[] serializedValue = null;
        ObjectMapper om = new ObjectMapper();
        if (data != null) {
            try {
                serializedValue = om.writeValueAsString(data).getBytes();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        return serializedValue;
    }
}
