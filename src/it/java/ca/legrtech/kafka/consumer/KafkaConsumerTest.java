package ca.legrtech.kafka.consumer;

import ca.legrtech.kafka.config.IntegrationTestParent;
import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventBodyBuilder;
import ca.legrtech.kafka.event.EventKey;
import ca.legrtech.kafka.event.EventKeyBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerTest extends IntegrationTestParent {

    private static final String EVENT_TYPE = "eventType";
    private static final String OBJECT_ID = "anObjectId";
    private static final byte[] DATA = {0, 1};

    @Mock
    private EventHandler eventHandler;

    private KafkaConsumer consumer;

    @BeforeEach
    void setupConsumer() {
        consumer = KafkaConsumerBuilder.newBuilder()
                                       .withBootstrapServers(KAFKA.getBootstrapServers())
                                       .withTopic(CONSUMER_TOPIC)
                                       .withGroupId("consumer-group-id")
                                       .withHandler(EVENT_TYPE, eventHandler)
                                       .build();
        consumer.start();
    }

    @AfterEach
    void stopConsumer() {
        consumer.stop();
    }

    @Test
    void givenEventWithHandledType_whenConsume_thenPrintMessage() throws Exception {
        EventKey key = EventKeyBuilder.newBuilder()
                                      .withObjectId(OBJECT_ID)
                                      .build();
        EventBody eventBody = EventBodyBuilder.newBuilder()
                                              .withSource("/source")
                                              .withType(EVENT_TYPE)
                                              .withDatacontenttype("byte")
                                              .withData(DATA)
                                              .build();

        kafkaProducerFixture.send(key, eventBody);

        ArgumentCaptor<ConsumerRecord<EventKey, EventBody>> captor = ArgumentCaptor.forClass(ConsumerRecord.class);
        verify(eventHandler, timeout(100)).handle(captor.capture());
        assertThat(captor.getValue().key()).isEqualTo(key);
        assertThat(captor.getValue().value()).isEqualTo(eventBody);
    }
}