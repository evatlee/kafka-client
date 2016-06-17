package com.example.consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Consumes and processes messages from Kafka.
 *
 * @param <K>
 *         key type
 * @param <V>
 *         value type
 */
public abstract class AbstractKafkaMessageProcessor<K, V> {

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<String> topics;
    private final ExecutorService executor;

    protected AbstractKafkaMessageProcessor(
            KafkaConsumer<K, V> kafkaConsumer,
            List<String> topics) {

        this.kafkaConsumer = kafkaConsumer;
        this.topics = topics;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(getClass().getSimpleName() + "-%d")
                .build();
        executor = Executors.newFixedThreadPool(1, threadFactory);
    }

    /**
     * Processes message.
     *
     * @param record
     *         to process
     */
    protected abstract void process(ConsumerRecord<K, V> record);

    /**
     * Starts consuming and processing messages.
     */
    public void start() {
        executor.execute(() -> {
            try {
                kafkaConsumer.subscribe(topics);

                while (true) {
                    ConsumerRecords<K, V> records = kafkaConsumer.poll(Long.MAX_VALUE);
                    records.forEach(record -> process(record));
                    kafkaConsumer.commitSync();
                }
            } catch (WakeupException e) {
                // Ignore. This exception signals we are stopping.
            } finally {
                kafkaConsumer.close();
            }
        });
    }

    /**
     * Stops consuming and processing messages.
     */
    public void stop() {
        kafkaConsumer.wakeup();
        executor.shutdown();
    }
}
