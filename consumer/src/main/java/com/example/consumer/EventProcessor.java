package com.example.consumer;

import com.example.reflect.Event;
import com.tenx.ms.commons.kafka.AbstractReflectKafkaAvroDeserializer;
import com.tenx.ms.commons.kafka.KafkaConsumerBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Consumes from Kafka.
 */
@Component
public class EventProcessor extends AbstractKafkaMessageProcessor<String, Event> {

    public static class EventDeserializer extends AbstractReflectKafkaAvroDeserializer<Event> {
        @Override
        protected Class<Event> getType() {
            return Event.class;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    private static final String GROUP_ID = Event.class.getSimpleName();
    private static final String TOPIC = Event.class.getSimpleName();

    @Autowired
    public EventProcessor(KafkaConsumerBuilder kafkaConsumerBuilder) {
        super(
                kafkaConsumerBuilder
                        .setValueDeserializer(EventDeserializer.class)
                        .setGroupId(GROUP_ID)
                        .build(),
                Arrays.asList(TOPIC));
    }

    @Override
    protected void process(ConsumerRecord<String, Event> record) {
        Event event = record.value();
        LOGGER.info(event.toString());
    }
}
