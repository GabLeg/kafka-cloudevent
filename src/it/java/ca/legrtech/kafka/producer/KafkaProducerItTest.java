package ca.legrtech.kafka.producer;

import ca.legrtech.kafka.config.IntegrationTestParent;
import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventBodyBuilder;
import ca.legrtech.kafka.event.EventKey;
import ca.legrtech.kafka.event.EventKeyBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

class KafkaProducerItTest extends IntegrationTestParent {

    private static final String OBJECT_ID = "anObjectId";
    private static final byte[] DATA = {0, 1};

    private KafkaProducer kafkaProducer;

    @BeforeEach
    void setupProducer() {
        kafkaProducer = KafkaProducerBuilder.newBuilder()
                                            .withBootstrapServers(KAFKA.getBootstrapServers())
                                            .withTopic(PRODUCER_TOPIC)
                                            .build();

    }

    @Test
    void givenEventBodyWithEvenKey_whenSend_thenEventIsSentInTopic() {
        EventKey key = EventKeyBuilder.newBuilder()
                                      .withObjectId(OBJECT_ID)
                                      .build();
        EventBody eventBody = EventBodyBuilder.newBuilder()
                                              .withSource("/source")
                                              .withType("newType")
                                              .withDatacontenttype("byte")
                                              .withData(DATA)
                                              .build();

        kafkaProducer.send(key, eventBody);

        ConsumerRecord<EventKey, EventBody> record = kafkaConsumerFixture.pollFirst();
        assertThat(record.key()).isEqualTo(key);
        assertThat(record.value()).isEqualTo(eventBody);
    }

    @Test
    void givenEventBody_whenSend_thenEventIsSentInTopic() {
        EventBody eventBody = EventBodyBuilder.newBuilder()
                                              .withSource("/source")
                                              .withType("newType")
                                              .withDatacontenttype("byte")
                                              .withData(DATA)
                                              .build();

        kafkaProducer.send(eventBody);

        ConsumerRecord<EventKey, EventBody> record = kafkaConsumerFixture.pollFirst();
        assertThat(record.value()).isEqualTo(eventBody);
    }

}