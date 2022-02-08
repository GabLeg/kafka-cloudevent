package ca.legrtech.kafka.deserializer;

import org.apache.kafka.common.KafkaException;

public class DeserializationException extends KafkaException {

    private static final long serialVersionUID = 3L;

    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DeserializationException(String message) {
        super(message);
    }
}
