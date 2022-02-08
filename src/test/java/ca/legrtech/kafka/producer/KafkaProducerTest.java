package ca.legrtech.kafka.producer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventBodyBuilder;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTest {

    private static final String TOPIC = "testTopic";
    private static final String SOURCE = "source";
    private static final String TYPE = "type";
    private static final EventBody EVENT_BODY = EventBodyBuilder.newBuilder()
                                                                .withSource(SOURCE)
                                                                .withType(TYPE)
                                                                .build();

    @Mock
    private org.apache.kafka.clients.producer.KafkaProducer<EventKey, EventBody> kafkaProducer;

    @Mock
    private RecordMetadata recordMetadata;

    private KafkaProducer producer;

    @BeforeEach
    void setup() {
        producer = new KafkaProducer(kafkaProducer, TOPIC);
    }

    @Test
    void givenEventBody_whenSend_thenReturnRecordMetadata() {
        Future<RecordMetadata> future = CompletableFuture.completedFuture(recordMetadata);
        when(kafkaProducer.send(any())).thenReturn(future);

        RecordMetadata metadata = producer.send(EVENT_BODY);

        assertThat(metadata).isEqualTo(recordMetadata);
    }

    @Test
    void givenExceptionWhileSending_whenSend_thenThrowKafkaProducerException() {
        when(kafkaProducer.send(any())).thenThrow(KafkaException.class);

        Executable send = () -> producer.send(EVENT_BODY);

        assertThrows(KafkaProducerException.class, send);
    }
}