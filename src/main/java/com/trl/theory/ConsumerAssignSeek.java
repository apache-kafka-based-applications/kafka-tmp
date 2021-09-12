package com.trl.theory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class ConsumerAssignSeek {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerAssignSeek.class);

    private static final Properties CONSUMER_PROPERTIES = new Properties();

    private static final String TOPIC_NAME = "t1";
    private static final String AUTO_OFFSET = "earliest";
//    private static final String AUTO_OFFSET = "latest";
//    private static final String AUTO_OFFSET = "none";

    public static void main(String[] args) {
        CONSUMER_PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        CONSUMER_PROPERTIES.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.setProperty(AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(CONSUMER_PROPERTIES);

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        final TopicPartition topicPartition_0 = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Collections.singleton(topicPartition_0));

        // seek
        final long offsetToReadFrom = 15L;
        consumer.seek(topicPartition_0, offsetToReadFrom);

        final int numberOfMessagesToRead = 5;
        boolean keepOnRead = true;
        int numberOfMessagesToReadSoFar = 0;

        // poll for new data
        while (keepOnRead) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: consumerRecords) {
                numberOfMessagesToReadSoFar++;
                final String message = "Key: [{}] Value: [{}] Partition: [{}] Offset: [{}]";
                LOGGER.info(message, record.key(), record.value(), record.partition(), record.offset());
                if (numberOfMessagesToReadSoFar >= numberOfMessagesToRead) {
                    keepOnRead = false;
                }
            }

        }

        consumer.close();
    }

}
