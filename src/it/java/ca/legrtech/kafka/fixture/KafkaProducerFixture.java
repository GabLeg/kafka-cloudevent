package ca.legrtech.kafka.fixture;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerFixture {

    private final KafkaProducer<EventKey, EventBody> producer;
    private final String topic;

    public KafkaProducerFixture(String bootstrapServers, String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.RETRIES_CONFIG, 2147483647);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.serializer.EventKeySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.serializer.EventBodySerializer");
        producer = new KafkaProducer<>(properties);
    }

    public void send(EventBody body) {
        send(null, body);
    }

    public void send(EventKey key, EventBody body) {
        ProducerRecord<EventKey, EventBody> record = new ProducerRecord<>(topic, key, body);
        producer.send(record);
    }
}
