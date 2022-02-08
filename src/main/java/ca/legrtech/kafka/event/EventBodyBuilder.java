package ca.legrtech.kafka.event;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class EventBodyBuilder {

    private String id;
    private String traceid;
    private String specversion;
    private String source;
    private String type;
    private OffsetDateTime time;
    private String datacontenttype;
    private byte[] data;
    private final Map<String, String> extensions;

    private EventBodyBuilder() {
        id = UUID.randomUUID().toString();
        traceid = UUID.randomUUID().toString();
        specversion = "1.0";
        time = OffsetDateTime.now();
        extensions = new HashMap<>();
    }

    public static EventBodyBuilder newBuilder() {
        return new EventBodyBuilder();
    }

    public EventBodyBuilder withId(String id) {
        this.id = id;
        return this;
    }

    public EventBodyBuilder withTraceid(String traceid) {
        this.traceid = traceid;
        return this;
    }

    public EventBodyBuilder withSpecversion(String specversion) {
        this.specversion = specversion;
        return this;
    }

    public EventBodyBuilder withSource(String source) {
        this.source = source;
        return this;
    }

    public EventBodyBuilder withType(String type) {
        this.type = type;
        return this;
    }

    public EventBodyBuilder withTime(OffsetDateTime time) {
        this.time = time;
        return this;
    }

    public EventBodyBuilder withDatacontenttype(String datacontenttype) {
        this.datacontenttype = datacontenttype;
        return this;
    }

    public EventBodyBuilder withData(byte[] data) {
        this.data = data;
        return this;
    }

    public EventBodyBuilder withExtension(String key, String value) {
        this.extensions.put(key, value);
        return this;
    }

    public EventBody build() {
        if (source == null) {
            throw new IllegalStateException("Attribute 'source' cannot be null.");
        }
        if (type == null) {
            throw new IllegalStateException("Attribute 'type' cannot be null.");
        }
        return new EventBody(id, traceid, specversion, source, type, time, datacontenttype, data, extensions);
    }
}
