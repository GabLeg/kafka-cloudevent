package ca.legrtech.kafka.serializer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventBodyField;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serializer;

import java.time.format.DateTimeFormatter;
import java.util.Map;

public class EventBodySerializer implements Serializer<EventBody> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private final ObjectMapper objectMapper;

    public EventBodySerializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, EventBody eventBody) {
        if (eventBody == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(toJson(eventBody));
        } catch (Exception e) {
            throw new SerializationException("Error serializing EventBody.", e);
        }
    }

    private ObjectNode toJson(EventBody eventBody) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put(EventBodyField.ID, eventBody.getId());
        root.put(EventBodyField.TRACE_ID, eventBody.getTraceid());
        root.put(EventBodyField.SPEC_VERSION, eventBody.getSpecversion());
        root.put(EventBodyField.SOURCE, eventBody.getSource());
        root.put(EventBodyField.TYPE, eventBody.getType());
        root.put(EventBodyField.TIME, eventBody.getTime().format(DATE_FORMAT));
        root.put(EventBodyField.DATA_CONTENT_TYPE, eventBody.getDatacontenttype());
        root.put(EventBodyField.DATA, eventBody.getData());
        Map<String, String> extensions = eventBody.getExtensions();
        for (String key : extensions.keySet()) {
            root = root.put(key, extensions.get(key));
        }
        return root;
    }
}
