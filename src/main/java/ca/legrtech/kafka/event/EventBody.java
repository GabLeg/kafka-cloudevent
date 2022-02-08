package ca.legrtech.kafka.event;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class EventBody {

    private final String id;
    private final String traceid;
    private final String specversion;
    private final String source;
    private final String type;
    private final OffsetDateTime time;
    private final String datacontenttype;
    private final byte[] data;
    private final Map<String, String> extensions;

    public EventBody(String id,
                     String traceid,
                     String specversion,
                     String source,
                     String type,
                     OffsetDateTime time,
                     String datacontenttype,
                     byte[] data,
                     Map<String, String> extensions)
    {
        this.id = id;
        this.traceid = traceid;
        this.specversion = specversion;
        this.source = source;
        this.type = type;
        this.time = time;
        this.datacontenttype = datacontenttype;
        this.data = data;
        this.extensions = extensions;
    }

    public String getId() {
        return id;
    }

    public String getTraceid() {
        return traceid;
    }

    public String getSpecversion() {
        return specversion;
    }

    public String getSource() {
        return source;
    }

    public String getType() {
        return type;
    }

    public OffsetDateTime getTime() {
        return time;
    }

    public String getDatacontenttype() {
        return datacontenttype;
    }

    public byte[] getData() {
        return data;
    }

    public Map<String, String> getExtensions() {
        return extensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventBody eventBody = (EventBody) o;
        return id.equals(eventBody.id) && Objects.equals(traceid, eventBody.traceid) && specversion.equals(
                eventBody.specversion) && source.equals(eventBody.source) && type.equals(eventBody.type) && Objects.equals(time.truncatedTo(
                                                                                                                                   ChronoUnit.MILLIS),
                                                                                                                           eventBody.time.truncatedTo(
                                                                                                                                   ChronoUnit.MILLIS)) && Objects.equals(
                datacontenttype, eventBody.datacontenttype) && Arrays.equals(data, eventBody.data) && Objects.equals(
                extensions, eventBody.extensions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, traceid, specversion, source, type, time, datacontenttype, extensions);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "EventBody{" +
                "id='" + id + '\'' +
                ", traceid='" + traceid + '\'' +
                ", specversion='" + specversion + '\'' +
                ", source='" + source + '\'' +
                ", type='" + type + '\'' +
                ", time=" + time +
                ", datacontenttype='" + datacontenttype + '\'' +
                ", data=" + Arrays.toString(data) +
                ", extensions=" + extensions +
                '}';
    }
}
