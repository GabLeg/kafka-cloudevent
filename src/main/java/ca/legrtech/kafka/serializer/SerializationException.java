package ca.legrtech.kafka.serializer;

import org.apache.kafka.common.KafkaException;

public class SerializationException extends KafkaException {

    private static final long serialVersionUID = 2L;

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerializationException(String message) {
        super(message);
    }
}
