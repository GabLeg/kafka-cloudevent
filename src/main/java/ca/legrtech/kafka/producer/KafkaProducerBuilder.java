package ca.legrtech.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public final class KafkaProducerBuilder {

    private final Properties properties;
    private String topic;

    private KafkaProducerBuilder() {
        properties = new Properties();
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.RETRIES_CONFIG, 2147483647);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.serializer.EventKeySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.serializer.EventBodySerializer");
    }

    public static KafkaProducerBuilder newBuilder() {
        return new KafkaProducerBuilder();
    }

    public KafkaProducerBuilder withBootstrapServers(String bootstrapServers) {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return this;
    }

    public KafkaProducerBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaProducerBuilder withConfig(String key, String value) {
        properties.put(key, value);
        return this;
    }

    public KafkaProducer build() {
        if (properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalStateException("Attribute '" + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "' cannot be null.");
        }
        if (topic == null) {
            throw new IllegalStateException("Attribute 'topic' cannot be null.");
        }
        return new KafkaProducer(new org.apache.kafka.clients.producer.KafkaProducer<>(properties), topic);
    }
}
