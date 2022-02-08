package ca.legrtech.kafka.consumer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.*;

public final class KafkaConsumerBuilder {

    private final Map<String, EventHandler> handlers;
    private final Properties properties;
    private Duration pollDuration;
    private List<String> topics;

    private KafkaConsumerBuilder() {
        handlers = new HashMap<>();
        pollDuration = Duration.ofMillis(1000);
        topics = new ArrayList<>();
        properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.deserializer.EventKeyDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.deserializer.EventBodyDeserializer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    }

    public static KafkaConsumerBuilder newBuilder() {
        return new KafkaConsumerBuilder();
    }

    public KafkaConsumerBuilder withHandler(String eventType, EventHandler handler) {
        handlers.put(eventType, handler);
        return this;
    }

    public KafkaConsumerBuilder withBootstrapServers(String bootstrapServers) {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return this;
    }

    public KafkaConsumerBuilder withGroupId(String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return this;
    }

    public KafkaConsumerBuilder withTopic(String topic) {
        topics.add(topic);
        return this;
    }

    public KafkaConsumerBuilder withTopics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    public KafkaConsumerBuilder withPollDuration(Duration pollDuration) {
        this.pollDuration = pollDuration;
        return this;
    }

    public KafkaConsumerBuilder withConfig(String key, String value) {
        properties.setProperty(key, value);
        return this;
    }


    public KafkaConsumer build() {
        if (properties.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new IllegalStateException("Attribute '" + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + "' cannot be null.");
        }
        if (properties.get(ConsumerConfig.GROUP_ID_CONFIG) == null) {
            throw new IllegalStateException("Attribute '" + ConsumerConfig.GROUP_ID_CONFIG + "' cannot be null.");
        }
        if (topics == null || topics.isEmpty()) {
            throw new IllegalStateException("Attribute 'topics' cannot be null or empty.");
        }
        org.apache.kafka.clients.consumer.KafkaConsumer<EventKey, EventBody> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                properties);
        kafkaConsumer.subscribe(topics);
        return new KafkaConsumer(kafkaConsumer, handlers, pollDuration);
    }
}
