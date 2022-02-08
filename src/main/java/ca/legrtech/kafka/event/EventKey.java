package ca.legrtech.kafka.event;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class EventKey {

    private final Map<String, String> keyFields;

    public EventKey(Map<String, String> keyFields) {
        this.keyFields = keyFields;
    }

    public String getField(String key) {
        return this.keyFields.get(key);
    }

    public Set<String> getKeys() {
        return this.keyFields.keySet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventKey eventKey = (EventKey) o;
        return keyFields.equals(eventKey.keyFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyFields);
    }
}
