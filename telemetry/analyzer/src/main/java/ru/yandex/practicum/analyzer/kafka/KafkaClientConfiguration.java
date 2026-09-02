package ru.yandex.practicum.analyzer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaClientConfiguration {

    @Bean(destroyMethod = "")
    Consumer<String, HubEventAvro> hubEventConsumer(KafkaProperties properties) {
        return new KafkaConsumer<>(new HashMap<String, Object>(properties.hubEventConsumer()));
    }

    @Bean(destroyMethod = "")
    Consumer<String, SensorsSnapshotAvro> snapshotConsumer(KafkaProperties properties) {
        return new KafkaConsumer<>(new HashMap<String, Object>(properties.snapshotConsumer()));
    }
}
