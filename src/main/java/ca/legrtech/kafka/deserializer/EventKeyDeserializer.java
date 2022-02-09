package ca.legrtech.kafka.deserializer;

import ca.legrtech.kafka.event.EventKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Iterator;
import java.util.TreeMap;

public class EventKeyDeserializer implements Deserializer<EventKey> {

    private final ObjectMapper objectMapper;

    public EventKeyDeserializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public EventKey deserialize(String topic, byte[] key) {
        if (key == null) {
            return null;
        }
        try {
            TreeMap<String, String> map = new TreeMap<>();
            ObjectNode root = (ObjectNode) objectMapper.readTree(key);
            Iterator<String> rootIterator = root.fieldNames();
            rootIterator.forEachRemaining(it -> map.put(it, root.get(it).textValue()));
            return new EventKey(map);
        } catch (Exception e) {
            throw new DeserializationException("Error deserializing EventKey.", e);
        }
    }
}
