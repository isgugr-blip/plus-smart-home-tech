package ru.yandex.practicum.collector.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties("collector.kafka")
public record KafkaProperties(Map<String, String> producer,
                              String sensorsTopic,
                              String hubsTopic) {
}
