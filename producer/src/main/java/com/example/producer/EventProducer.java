package com.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tenx.ms.commons.kafka.KafkaProducerBuilder;
import com.tenx.ms.lm.listing.rest.dto.Listing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Publish to Kafka.
 */
@Component
public class EventProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProducer.class);

    private static final String TOPIC = "LISTING_CREATED";

    @Autowired
    private KafkaProducerBuilder kafkaProducerBuilder;

    @Value("file:/Users/elee/github/kafka-client/producer/src/main/resources/config/createListing.json")
    private File createListingRequest;

    @Autowired
    private ObjectMapper mapper;

    private Producer<String, Listing> producer;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${avro.schema.registry.url}")
    private String schemaRegistryUrl;

    @PostConstruct
    @SuppressWarnings("PMD.UnusedPrivateMethod")
    private void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "com.tenx.ms.commons.kafka.ReflectKafkaAvroSerializer");
        props.put("value.serializer", "com.tenx.ms.commons.kafka.ReflectKafkaAvroSerializer");
        props.put("request.required.acks", "1");
        props.put("schema.registry.url", schemaRegistryUrl);
        producer = new KafkaProducer<>(props);
    }

    public void run() {

        Listing listing = null;
        try {
            listing = mapper.readValue(createListingRequest, Listing.class);
        } catch (IOException e) {
            LOGGER.error("Couldn't parse JSON file");
        }

//        KafkaProducer<String, Listing> kafkaProducer = kafkaProducerBuilder
//                .setValueSerializer(ReflectKafkaAvroSerializer.class)
//                .build();

        //Listing listing = new Listing();
        for (int i = 1; i <= 10; ++i) {

            listing.setListingId((long)i);
            listing.setListingStatus("Approved");
            //listing.getListingSignage().setListingSignageId(1L);
//            listing.setListingStatus("Approved");
//            listing.setLeaderboardVisible(false);
//            listing.setSalesChannelId(101);

            ProducerRecord<String, Listing> record = new ProducerRecord<>(TOPIC, listing);
            LOGGER.debug("Try to send listing to kafka topic {}", TOPIC);
            producer.send(record);
        }

        producer.close();
    }
}
