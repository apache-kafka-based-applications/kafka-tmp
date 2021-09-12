package com.trl.theory;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    private static final Properties CONSUMER_PROPERTIES = new Properties();

    private static final String TOPIC_NAME = "t1";
    private static final String GROUP_ID = "g1";
    private static final String AUTO_OFFSET = "earliest";
//    private static final String AUTO_OFFSET = "latest";
//    private static final String AUTO_OFFSET = "none";

    public static void main(String[] args) {
        CONSUMER_PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        CONSUMER_PROPERTIES.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.setProperty(GROUP_ID_CONFIG, GROUP_ID);
        CONSUMER_PROPERTIES.setProperty(AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET);
//        CONSUMER_PROPERTIES.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true");
//        CONSUMER_PROPERTIES.setProperty(MAX_POLL_RECORDS_CONFIG, "10");

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(CONSUMER_PROPERTIES)){
            consumer.subscribe(Collections.singleton(TOPIC_NAME));

            while (true) {
                final ConsumerRecords<String, String> re = consumer.poll(Duration.ofMillis(100));

                re.forEach(record -> {
                    final String message = "Key: [{}] Value: [{}] Partition: [{}] Offset: [{}]";
                    LOGGER.info(message, record.key(), record.value(), record.partition(), record.offset());
                });
            }
        }

    }

}
