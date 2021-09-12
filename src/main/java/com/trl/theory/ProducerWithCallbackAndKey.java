package com.trl.theory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerWithCallbackAndKey {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerWithCallbackAndKey.class);

    private static final Properties PRODUCER_PROPERTIES = new Properties();

    private static final String TOPIC_NAME = "t1";

    public static void main(String[] args) {
        PRODUCER_PROPERTIES.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        PRODUCER_PROPERTIES.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        PRODUCER_PROPERTIES.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_PROPERTIES);

        IntStream.range(1, 31).forEach(valueInt -> {
            final String value = "Hello World!!! -> " + valueInt;
            final String key = "Key " + valueInt;
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, key, value);

            producer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    final String message = "Event is send correctly. Topic name: [{}] Partition: [{}] Offset: [{}] Timestamp: [{}]";
                    LOGGER.info(message, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    LOGGER.error(exception.getMessage(), exception);
                }
            });
        });

        // flush
        producer.flush();

        // flush and close
        producer.close();

    }

}
