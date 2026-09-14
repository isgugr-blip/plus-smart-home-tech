package ru.yandex.practicum.analyzer.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

@ConfigurationProperties("analyzer.kafka")
public record KafkaProperties(Map<String, String> hubEventConsumer,
                              Map<String, String> snapshotConsumer,
                              String hubsTopic,
                              String snapshotsTopic,
                              Duration pollTimeout) {
}
