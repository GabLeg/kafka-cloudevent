package ca.legrtech.kafka.consumer;

import ca.legrtech.kafka.event.EventBody;
import ca.legrtech.kafka.event.EventKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface EventHandler {

    void handle(ConsumerRecord<EventKey, EventBody> record);
}
