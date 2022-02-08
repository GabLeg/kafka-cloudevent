package ca.legrtech.kafka.producer;

public class KafkaProducerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public KafkaProducerException(Throwable throwable) {
        super(throwable);
    }
}
