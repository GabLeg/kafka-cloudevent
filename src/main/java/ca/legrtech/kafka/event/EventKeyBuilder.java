package ca.legrtech.kafka.event;

import java.util.TreeMap;

public final class EventKeyBuilder {

    private static final String OBJECT_ID = "objectId";

    private final TreeMap<String, String> keyFields;

    private EventKeyBuilder() {
        keyFields = new TreeMap<>();
    }

    public static EventKeyBuilder newBuilder() {
        return new EventKeyBuilder();
    }

    public EventKeyBuilder withObjectId(String objectId) {
        keyFields.put(OBJECT_ID, objectId);
        return this;
    }

    public EventKeyBuilder withExtension(String key, String value) {
        keyFields.put(key, value);
        return this;
    }

    public EventKey build() {
        return new EventKey(keyFields);
    }
}
