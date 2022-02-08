package ca.legrtech.kafka.fixture;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerFixture {

    private static final Duration POLL_DURATION = Duration.ofMillis(1000);

    private final KafkaConsumer<EventKey, EventBody> consumer;

    public KafkaConsumerFixture(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "fixture-group-id");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.deserializer.EventKeyDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "ca.legrtech.kafka.deserializer.EventBodyDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public ConsumerRecord<EventKey, EventBody> pollFirst() {
        ConsumerRecords<EventKey, EventBody> records = poll();
        if (records.isEmpty()) {
            return null;
        }
        return records.iterator().next();
    }

    public ConsumerRecords<EventKey, EventBody> poll() {
        ConsumerRecords<EventKey, EventBody> records = consumer.poll(POLL_DURATION);
        consumer.commitSync();
        return records;
    }

    public void reset() {
        ConsumerRecords<EventKey, EventBody> records;
        do {
            records = poll();
        } while (!records.isEmpty());
    }

    public void close() {
        consumer.close();
    }
}
