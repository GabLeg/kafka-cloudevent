package ca.legrtech.kafka.consumer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final org.apache.kafka.clients.consumer.KafkaConsumer<EventKey, EventBody> consumer;
    private final Map<String, EventHandler> handlers;
    private final Duration pollDuration;
    private final AtomicBoolean isRunning;

    public KafkaConsumer(org.apache.kafka.clients.consumer.KafkaConsumer<EventKey, EventBody> consumer,
                         Map<String, EventHandler> handlers,
                         Duration pollDuration
    ) {
        this.consumer = consumer;
        this.handlers = handlers;
        this.pollDuration = pollDuration;
        this.isRunning = new AtomicBoolean(false);
    }

    public void start() {
        Thread thread = new Thread(this::startListening);
        thread.start();
    }

    public void stop() {
        isRunning.set(false);
    }

    private void startListening() {
        isRunning.set(true);
        while (isRunning.get()) {
            try {
                readEvents();
            } catch (Exception e) {
                logger.warn("An error happen while reading events", e);
            }
        }
    }

    private void readEvents() {
        ConsumerRecords<EventKey, EventBody> records = consumer.poll(pollDuration);
        for (ConsumerRecord<EventKey, EventBody> event : records) {
            String type = event.value().getType();
            EventHandler eventHandler = handlers.get(type);
            if (eventHandler == null) {
                logger.info("No handler found for event [{}].", type);
            } else {
                eventHandler.handle(event);
            }
        }
    }
}
