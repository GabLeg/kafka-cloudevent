package ca.legrtech.kafka.config;

import ca.legrtech.kafka.fixture.KafkaConsumerFixture;
import ca.legrtech.kafka.fixture.KafkaProducerFixture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class IntegrationTestParent {

    protected static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
    protected static final String PRODUCER_TOPIC = "producer.test";
    protected static final String CONSUMER_TOPIC = "consumer.test";

    protected static KafkaConsumerFixture kafkaConsumerFixture;
    protected static KafkaProducerFixture kafkaProducerFixture;

    @BeforeAll
    static void setupKafka() {
        KAFKA.start();
        kafkaConsumerFixture = new KafkaConsumerFixture(KAFKA.getBootstrapServers(), PRODUCER_TOPIC);
        kafkaProducerFixture = new KafkaProducerFixture(KAFKA.getBootstrapServers(), CONSUMER_TOPIC);
    }

    @BeforeEach
    void resetEverything() {
        kafkaConsumerFixture.reset();
    }

    @AfterAll
    static void stopKafka() {
        kafkaConsumerFixture.close();
        KAFKA.stop();
    }
}
