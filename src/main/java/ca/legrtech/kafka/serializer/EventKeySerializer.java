package ca.legrtech.kafka.serializer;

import ca.legrtech.kafka.event.EventKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;

public class EventKeySerializer implements Serializer<EventKey> {

    private final ObjectMapper objectMapper;

    public EventKeySerializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, EventKey eventKey) {
        if (eventKey == null) {
            return null;
        }
        try {
            ObjectNode root = objectMapper.createObjectNode();
            for (String key : eventKey.getKeys()) {
                root = root.put(key, eventKey.getField(key));
            }
            return objectMapper.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new SerializationException("Error serializing EventKey.", e);
        }
    }
}
