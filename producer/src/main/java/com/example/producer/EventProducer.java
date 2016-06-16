package com.example.producer;

import com.example.reflect.Event;
import com.tenx.ms.commons.kafka.KafkaProducerBuilder;
import com.tenx.ms.commons.kafka.ReflectKafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Publish to Kafka.
 */
@Component
public class EventProducer {

    private static final String TOPIC = Event.class.getSimpleName();

    @Autowired
    private KafkaProducerBuilder kafkaProducerBuilder;

    public void run() {
        KafkaProducer<String, Event> kafkaProducer = kafkaProducerBuilder
                .setValueSerializer(ReflectKafkaAvroSerializer.class)
                .build();

        Event event = new Event();
        for (int i = 1; i <= 10; ++i) {
            event.setTimestamp(Instant.now().toString());
            event.setMessage(String.format("message %d", i));

            ProducerRecord<String, Event> record = new ProducerRecord<>(TOPIC, event);
            kafkaProducer.send(record);
        }

        kafkaProducer.close();
    }
}
