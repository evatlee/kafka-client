package com.example.consumer;

import com.example.reflect.Event;
import com.tenx.ms.commons.kafka.KafkaConsumerBuilder;
import com.tenx.ms.commons.kafka.AbstractReflectKafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumes from Kafka.
 */
@Component
public class EventConsumer {

    public static class EventDeserializer extends AbstractReflectKafkaAvroDeserializer<Event> {
        @Override
        protected Class<Event> getType() {
            return Event.class;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private static final String GROUP_ID = Event.class.getSimpleName();
    private static final String TOPIC = Event.class.getSimpleName();

    @Autowired
    private KafkaConsumerBuilder kafkaConsumerBuilder;

    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    private void process(Event event) {
        LOGGER.info(event.toString());
    }

    public void run() {
        KafkaConsumer<String, Event> kafkaConsumer = kafkaConsumerBuilder
                .setValueDeserializer(EventDeserializer.class)
                .setGroupId(GROUP_ID)
                .build();
        try {
            kafkaConsumer.subscribe(Arrays.asList(TOPIC));

            while (!shutdown.get()) {
                ConsumerRecords<String, Event> records = kafkaConsumer.poll(Long.MAX_VALUE);
                records.forEach(record -> process(record.value()));
                kafkaConsumer.commitSync();
            }
        } finally {
            kafkaConsumer.close();
            shutdownLatch.countDown();
        }
    }
}
