package ca.legrtech.kafka.deserializer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventBodyBuilder;
import ca.legrtech.kafka.event.EventBodyField;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EventBodyDeserializer implements Deserializer<EventBody> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final List<String> NOT_EXTENSIONS_KEYS = List.of(EventBodyField.ID, EventBodyField.TRACE_ID, EventBodyField.SPEC_VERSION,
                                                                    EventBodyField.SOURCE, EventBodyField.TYPE, EventBodyField.TIME,
                                                                    EventBodyField.DATA_CONTENT_TYPE, EventBodyField.DATA);

    private final ObjectMapper objectMapper;

    public EventBodyDeserializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public EventBody deserialize(String topic, byte[] body) {
        if (body == null) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            return toEventBody(root);
        } catch (Exception e) {
            throw new DeserializationException("Error deserializing EventBody.", e);
        }
    }

    private EventBody toEventBody(JsonNode root) throws Exception {
        EventBodyBuilder builder = EventBodyBuilder.newBuilder()
                                                   .withId(root.get(EventBodyField.ID).asText())
                                                   .withTraceid(root.get(EventBodyField.TRACE_ID).asText())
                                                   .withSpecversion(root.get(EventBodyField.SPEC_VERSION).asText())
                                                   .withSource(root.get(EventBodyField.SOURCE).asText())
                                                   .withType(root.get(EventBodyField.TYPE).asText())
                                                   .withTime(OffsetDateTime.parse(root.get(EventBodyField.TIME).asText(), DATE_FORMAT))
                                                   .withDatacontenttype(root.get(EventBodyField.DATA_CONTENT_TYPE).asText())
                                                   .withData(root.get(EventBodyField.DATA).binaryValue());

        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = root.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String, JsonNode> next = fieldsIterator.next();
            if (!NOT_EXTENSIONS_KEYS.contains(next.getKey())) {
                builder.withExtension(next.getKey(), next.getValue().asText());
            }
        }
        return builder.build();
    }
}
