package com.trl.theory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ConsumerManualCommit {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerManualCommit.class);

    private static final Duration duration = Duration.ofSeconds(5);

    private static String topicName;
    private static Integer partition;

    public void startConsumer() {
    }

}
