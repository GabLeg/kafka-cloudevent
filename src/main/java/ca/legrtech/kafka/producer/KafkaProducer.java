package ca.legrtech.kafka.producer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class KafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private final org.apache.kafka.clients.producer.KafkaProducer<EventKey, EventBody> producer;
    private final String topic;

    public KafkaProducer(org.apache.kafka.clients.producer.KafkaProducer<EventKey, EventBody> producer,
                         String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public RecordMetadata send(EventBody body) {
        return send(null, body);
    }

    public RecordMetadata send(EventKey key, EventBody body) {
        try {
            ProducerRecord<EventKey, EventBody> record = new ProducerRecord<>(topic, key, body);
            logger.debug("Sending message in kafka topic [{}] with traceid [{}].", topic, body.getTraceid());
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            logger.debug("Message with traceid [{}] successfully sent in Kafka topic.", body.getTraceid());
            return metadata;
        } catch (Exception e) {
            logger.warn("Unable to send message with traceid [{}] in Kafka topic.", body.getTraceid());
            throw new KafkaProducerException(e);
        }
    }
}
