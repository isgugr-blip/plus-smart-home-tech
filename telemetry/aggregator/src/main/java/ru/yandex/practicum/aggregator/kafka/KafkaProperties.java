package ru.yandex.practicum.aggregator.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

@ConfigurationProperties("aggregator.kafka")
public record KafkaProperties(Map<String, String> consumer,
                              Map<String, String> producer,
                              String sensorsTopic,
                              String snapshotsTopic,
                              Duration pollTimeout) {
}
